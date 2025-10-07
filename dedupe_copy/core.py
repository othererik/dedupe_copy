"""Core application logic for dedupe_copy
"""

import datetime
import fnmatch
import logging
import os
import queue
import shutil
import tempfile
import threading
import time
from collections import Counter
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

from .config import CopyConfig, CopyJob, WalkConfig
from .disk_cache_dict import DefaultCacheDict
from .manifest import Manifest
from .path_rules import build_path_rules
from .threads import (
    HIGH_PRIORITY,
    LOW_PRIORITY,
    CopyThread,
    ProgressThread,
    ReadThread,
    ResultProcessor,
    WalkThread,
)
from .utils import _throttle_puts, ensure_logging_configured, lower_extension

logger = logging.getLogger(__name__)


def _walk_fs(
    read_paths: List[str],
    walk_config: WalkConfig,
    *,
    work_queue: "queue.Queue[str]",
    walk_queue: Optional["queue.Queue[str]"] = None,
    already_processed: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    walk_threads: int = 4,
    save_event: Optional[threading.Event] = None,
) -> None:
    if walk_queue is None:
        walk_queue = queue.Queue()
    walk_done = threading.Event()
    walkers = []
    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Starting {walk_threads} walk workers")
        )
    for _ in range(walk_threads):
        w = WalkThread(
            walk_queue,
            walk_done,
            walk_config=walk_config,
            work_queue=work_queue,
            already_processed=already_processed,
            progress_queue=progress_queue,
            save_event=save_event,
        )
        walkers.append(w)
        w.start()
    for src in read_paths:
        _throttle_puts(walk_queue.qsize())
        walk_queue.put(src)
    walk_done.set()
    for w in walkers:
        w.join()


def _extension_report(md5_data: Any, show_count: int = 10) -> int:
    """Print details for each extension, sorted by total size, return
    total size for all extensions
    """
    sizes: Counter[str] = Counter()
    extension_counts: Counter[str] = Counter()
    for key, info in md5_data.items():
        for items in info:
            extension = lower_extension(items[0])
            if not extension:
                extension = "no_extension"
            sizes[extension] += items[1]
            extension_counts[extension] += 1
    logger.info("Top %d extensions by size:", show_count)
    for key, _ in zip(
        sorted(sizes, key=lambda x: sizes.get(x, 0), reverse=True), range(show_count)
    ):
        logger.info("  %s: %s bytes", key, sizes[key])
    logger.info("Top %d extensions by count:", show_count)
    for key, _ in zip(
        sorted(
            extension_counts, key=lambda x: extension_counts.get(x, 0), reverse=True
        ),
        range(show_count),
    ):
        logger.info("  %s: %d", key, extension_counts[key])
    return sum(sizes.values())


# will need to clean this up later
# pylint: disable=too-many-branches
def find_duplicates(
    read_paths: List[str],
    work_queue: "queue.Queue[str]",
    result_queue: "queue.Queue[Tuple[str, int, float, str]]",
    manifest: Any,
    collisions: Any,
    *,
    walk_config: WalkConfig,
    result_src: Optional[str] = None,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    walk_threads: int = 4,
    read_threads: int = 8,
    keep_empty: bool = False,
    save_event: Optional[threading.Event] = None,
    walk_queue: Optional["queue.Queue[str]"] = None,
) -> Tuple[Any, Any]:
    """Find duplicate files by comparing checksums across directories."""
    work_stop_event = threading.Event()
    result_stop_event = threading.Event()
    result_processor = ResultProcessor(
        result_stop_event,
        result_queue,
        collisions,
        manifest,
        keep_empty=keep_empty,
        progress_queue=progress_queue,
        save_event=save_event,
    )
    result_processor.start()
    result_fh = None
    if result_src is not None:
        # pylint: disable=consider-using-with
        result_fh = open(result_src, "ab")
        result_fh.write(f"Src: {read_paths}\n".encode("utf-8"))
        result_fh.write("Collision #, MD5, Path, Size (bytes), mtime\n".encode("utf-8"))
    try:
        if progress_queue:
            progress_queue.put(
                (HIGH_PRIORITY, "message", f"Starting {read_threads} read workers")
            )
        work_threads = []
        for _ in range(read_threads):
            w = ReadThread(
                work_queue,
                result_queue,
                work_stop_event,
                progress_queue=progress_queue,
                save_event=save_event,
            )
            work_threads.append(w)
            w.start()
        _walk_fs(
            read_paths,
            walk_config,
            work_queue=work_queue,
            walk_queue=walk_queue,
            already_processed=manifest.read_sources,
            progress_queue=progress_queue,
            walk_threads=walk_threads,
            save_event=save_event,
        )
        while not work_queue.empty():
            if progress_queue:
                progress_queue.put(
                    (
                        HIGH_PRIORITY,
                        "message",
                        f"Waiting for work queue to empty: {work_queue.qsize()} "
                        f"items remain",
                    )
                )
            time.sleep(5)
        work_stop_event.set()
        # let the workers finish
        for worker in work_threads:
            worker.join()
        result_stop_event.set()
        while result_processor.is_alive():
            result_processor.join(5)  # wait for result processor to complete
        if collisions:
            group = 0
            logger.info("Hash Collisions:")
            for md5, info in collisions.items():
                group += 1
                logger.info("  MD5: %s", md5)
                for item in info:
                    logger.info("    %r, %s", item[0], item[1])
                    if result_fh:
                        line = (
                            f"{group}, {md5}, {repr(item[0])}, {item[1]}, {item[2]}\n"
                        )
                        result_fh.write(line.encode("utf-8"))
        else:
            logger.info("No Duplicates Found")
        return (collisions, manifest)
    finally:
        if result_fh:
            result_fh.close()


def info_parser(data: Any) -> Iterator[Tuple[str, str, str, int]]:
    """Yields (MD5, path, mtime_string, size) tuples from a md5_data
    dictionary"""
    if data:
        for md5, info in data.items():
            for item in info:
                try:
                    time_stamp = datetime.datetime.fromtimestamp(item[2])
                    year_month = f"{time_stamp.year}_{time_stamp.month:0>2}"
                except (OverflowError, OSError, ValueError) as e:
                    logger.error("ERROR: %r %s", item[0], e)
                    year_month = "Unknown"
                yield md5, item[0], year_month, item[1]


def queue_copy_work(
    copy_queue: "queue.Queue[Tuple[str, str, int]]",
    data: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    copied: Any,
    *,
    copy_job: "CopyJob",
) -> Any:
    """Queue copy operations for file duplication."""
    for md5, path, mtime, size in info_parser(data):
        if md5 not in copied:
            action_required = True
            if copy_job.ignore:
                for ignored_pattern in copy_job.ignore:
                    if fnmatch.fnmatch(path, ignored_pattern):
                        action_required = False
                        break
            if action_required:
                if not copy_job.ignore_empty_files:
                    copied[md5] = None
                elif md5 != "d41d8cd98f00b204e9800998ecf8427e":
                    copied[md5] = None
                _throttle_puts(copy_queue.qsize())
                copy_queue.put((path, mtime, size))
            elif progress_queue:
                progress_queue.put((LOW_PRIORITY, "not_copied", path))
        elif progress_queue:
            progress_queue.put((LOW_PRIORITY, "not_copied", path))
    return copied


def copy_data(
    dupes: Any,
    all_data: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    *,
    copy_job: "CopyJob",
) -> None:
    """Queues up the copy work, waits for threads to finish"""
    stop_event = threading.Event()
    copy_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue()
    workers = []
    copied = copy_job.no_copy
    if progress_queue:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                f"Starting {copy_job.copy_threads} copy workers",
            )
        )
    for _ in range(copy_job.copy_threads):
        c = CopyThread(
            copy_queue,
            stop_event,
            copy_config=copy_job.copy_config,
            progress_queue=progress_queue,
        )
        workers.append(c)
        c.start()
    if progress_queue:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                f"Copying to {copy_job.copy_config.target_path}",
            )
        )
    # copied is passed to here so we don't try to copy "comparison" manifests
    # copied is a dict-like, so it's update in place
    queue_copy_work(
        copy_queue,
        dupes,
        progress_queue,
        copied,
        copy_job=copy_job,
    )
    queue_copy_work(
        copy_queue,
        all_data,
        progress_queue,
        copied,
        copy_job=copy_job,
    )
    stop_event.set()
    for c in workers:
        c.join()
    if progress_queue and copied is not None:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Processed {len(copied)} unique items")
        )


# pylint: disable=too-many-statements
def run_dupe_copy(
    read_from_path: Optional[Union[str, List[str]]] = None,
    extensions: Optional[List[str]] = None,
    manifests_in_paths: Optional[Union[str, List[str]]] = None,
    manifest_out_path: Optional[str] = None,
    *,
    path_rules: Optional[List[str]] = None,
    copy_to_path: Optional[str] = None,
    ignore_old_collisions: bool = False,
    ignored_patterns: Optional[List[str]] = None,
    csv_report_path: Optional[str] = None,
    walk_threads: int = 4,
    read_threads: int = 8,
    copy_threads: int = 8,
    convert_manifest_paths_to: str = "",
    convert_manifest_paths_from: str = "",
    no_walk: bool = False,
    no_copy: Optional[List[str]] = None,
    keep_empty: bool = False,
    compare_manifests: Optional[Union[str, List[str]]] = None,
    preserve_stat: bool = False,
) -> None:
    """For external callers this is the entry point for dedupe + copy"""
    # Ensure logging is configured for programmatic calls
    ensure_logging_configured()

    # Display pre-flight summary
    logger.info("=" * 70)
    logger.info("DEDUPE COPY - Operation Summary")
    logger.info("=" * 70)
    if read_from_path:
        paths = read_from_path if isinstance(read_from_path, list) else [read_from_path]
        logger.info("Source path(s): %d path(s)", len(paths))
        for p in paths:
            logger.info("  - %s", p)
    if copy_to_path:
        logger.info("Destination: %s", copy_to_path)
    if manifests_in_paths:
        manifests = (
            manifests_in_paths
            if isinstance(manifests_in_paths, list)
            else [manifests_in_paths]
        )
        logger.info("Input manifest(s): %d manifest(s)", len(manifests))
    if manifest_out_path:
        logger.info("Output manifest: %s", manifest_out_path)
    if extensions:
        logger.info("Extension filter: %s", ", ".join(extensions))
    if ignored_patterns:
        logger.info("Ignored patterns: %s", ", ".join(ignored_patterns))
    if path_rules:
        logger.info("Path rules: %s", ", ".join(path_rules))
    logger.info(
        "Threads: walk=%d, read=%d, copy=%d", walk_threads, read_threads, copy_threads
    )
    logger.info(
        "Options: keep_empty=%s, preserve_stat=%s, no_walk=%s",
        keep_empty,
        preserve_stat,
        no_walk,
    )
    if compare_manifests:
        comp_list = (
            compare_manifests
            if isinstance(compare_manifests, list)
            else [compare_manifests]
        )
        logger.info("Compare manifests: %d manifest(s)", len(comp_list))
    logger.info("=" * 70)
    logger.info("")

    temp_directory = tempfile.mkdtemp(suffix="dedupe_copy")

    save_event = threading.Event()
    manifest = Manifest(
        manifests_in_paths,
        save_path=manifest_out_path,
        temp_directory=temp_directory,
        save_event=save_event,
    )
    compare = Manifest(compare_manifests, save_path=None, temp_directory=temp_directory)

    if no_copy:
        for item in no_copy:
            compare[item] = None

    if no_walk and not manifest:
        raise ValueError("If --no-walk is specified, a manifest must be supplied.")
    if read_from_path and not isinstance(read_from_path, list):
        read_from_path = [read_from_path]
    path_rules_func: Optional[Callable[..., Tuple[str, str]]] = None
    if path_rules:
        path_rules_func = build_path_rules(path_rules)
    all_stop = threading.Event()
    work_queue: "queue.Queue[str]" = queue.Queue()
    result_queue: "queue.Queue[Tuple[str, int, float, str]]" = queue.Queue()
    progress_queue: "queue.PriorityQueue[Any]" = queue.PriorityQueue()
    walk_queue: "queue.Queue[str]" = queue.Queue()
    progress_thread = ProgressThread(
        work_queue,
        result_queue,
        progress_queue,
        walk_queue=walk_queue,
        stop_event=all_stop,
        save_event=save_event,
    )
    progress_thread.start()
    collisions = None
    if manifest and (convert_manifest_paths_to or convert_manifest_paths_from):
        manifest.convert_manifest_paths(
            convert_manifest_paths_from, convert_manifest_paths_to
        )

    # storage for hash collisions
    collisions_file = os.path.join(temp_directory, "collisions.db")
    collisions = DefaultCacheDict(list, db_file=collisions_file, max_size=10000)
    if manifest and not ignore_old_collisions:
        # rebuild collision list
        for md5, info in manifest.iteritems():
            if len(info) > 1:
                collisions[md5] = info
    if no_walk:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Not walking file system. Using stored manifests",
            )
        )
        all_data = manifest
        dupes = collisions
    else:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Running the duplicate search, generating reports",
            )
        )
        walk_config = WalkConfig(extensions=extensions, ignore=ignored_patterns)
        dupes, all_data = find_duplicates(
            read_from_path or [],
            work_queue,
            result_queue,
            manifest,
            collisions,
            walk_config=walk_config,
            result_src=csv_report_path,
            progress_queue=progress_queue,
            walk_threads=walk_threads,
            read_threads=read_threads,
            keep_empty=keep_empty,
            save_event=save_event,
            walk_queue=walk_queue,
        )
    total_size = _extension_report(all_data)
    logger.info("Total Size of accepted: %s bytes", total_size)
    if manifest_out_path:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Saving complete manifest from search")
        )
        all_data.save(path=manifest_out_path)
    if copy_to_path is not None:
        # Warning: strip dupes out of all data, this assumes dupes correctly
        # follows handling of keep_empty (not a dupe even if md5 is same for
        # zero byte files)
        for md5 in dupes:
            if md5 in all_data:
                del all_data[md5]
        # copy the duplicate files first and then ignore them for the full pass
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Running copy to {repr(copy_to_path)}")
        )
        copy_config = CopyConfig(
            target_path=copy_to_path,
            read_paths=read_from_path or [],
            extensions=extensions,
            path_rules=path_rules_func,
            preserve_stat=preserve_stat,
        )
        copy_job = CopyJob(
            copy_config=copy_config,
            ignore=ignored_patterns,
            no_copy=compare,
            ignore_empty_files=keep_empty,
            copy_threads=copy_threads,
        )
        copy_data(
            dupes,
            all_data,
            progress_queue,
            copy_job=copy_job,
        )
    all_stop.set()
    while progress_thread.is_alive():
        progress_thread.join(5)
    del collisions
    try:
        time.sleep(1)
        shutil.rmtree(temp_directory)
    except OSError as err:
        logger.warning(
            "Failed to cleanup the collisions file: %s with err: %s",
            collisions_file,
            err,
        )
