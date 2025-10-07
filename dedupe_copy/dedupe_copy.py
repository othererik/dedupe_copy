"""Find duplicate files on a file system


work_queue -> files put on here by WalkThread(s), read by ReadThread(s)
result_queue -> Put to by ReadThreads(s) read by ResultProcessor
progress_queue -> read by ProgressThread and put to by all others
walk_queue -> holds directories discovered by WalkThread(s)
copy_queue -> read by CopyThread(s) put to by queue_copy_work
"""
# pylint: disable=too-many-arguments,too-many-instance-attributes,too-few-public-methods
# pylint: disable=too-many-locals,too-many-branches,too-many-statements

import datetime
import fnmatch
from collections import defaultdict
import logging
import os
import queue
import random
import shutil
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Tuple, Callable, Any, Iterator, Union, Set

from .disk_cache_dict import CacheDict, DefaultCacheDict
from .threads import (
    CopyThread,
    ResultProcessor,
    ReadThread,
    WalkThread,
    ProgressThread,
    HIGH_PRIORITY,
    LOW_PRIORITY,
)
from . import utils_dedupe

# Configure module logger
logger = logging.getLogger(__name__)


# Default logging setup for programmatic use (tests, API calls)
def _ensure_logging_configured() -> None:
    """Ensure logging is configured with sensible defaults if not already set"""
    root_logger = logging.getLogger("dedupe_copy")
    if not root_logger.handlers:
        # No handlers configured yet, set up basic configuration
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
        root_logger.propagate = False


class DedupeCopyConfig:
    """Configuration for a dedupe_copy operation."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        read_from_path: Optional[Union[str, List[str]]] = None,
        extensions: Optional[List[str]] = None,
        manifests_in_paths: Optional[Union[str, List[str]]] = None,
        manifest_out_path: Optional[str] = None,
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
    ):
        self.read_from_path = read_from_path
        self.extensions = extensions
        self.manifests_in_paths = manifests_in_paths
        self.manifest_out_path = manifest_out_path
        self.path_rules = path_rules
        self.copy_to_path = copy_to_path
        self.ignore_old_collisions = ignore_old_collisions
        self.ignored_patterns = ignored_patterns
        self.csv_report_path = csv_report_path
        self.walk_threads = walk_threads
        self.read_threads = read_threads
        self.copy_threads = copy_threads
        self.convert_manifest_paths_to = convert_manifest_paths_to
        self.convert_manifest_paths_from = convert_manifest_paths_from
        self.no_walk = no_walk
        self.no_copy = no_copy
        self.keep_empty = keep_empty
        self.compare_manifests = compare_manifests
        self.preserve_stat = preserve_stat
        if read_from_path:
            self.read_paths = (
                read_from_path if isinstance(read_from_path, list) else [read_from_path]
            )
        else:
            self.read_paths = []


def _walk_fs(
    config: "DedupeCopyConfig",
    queues: Dict[str, queue.Queue],
    already_processed: Any,
    save_event: Optional[threading.Event] = None,
) -> None:
    walk_queue = queues["walk"]
    work_queue = queues["work"]
    progress_queue = queues["progress"]
    walk_done = threading.Event()
    walkers = []
    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Starting %s walk workers", config.walk_threads)
        )
    for _ in range(config.walk_threads):
        w = WalkThread(
            config=config,
            walk_queue=walk_queue,
            stop_event=walk_done,
            work_queue=work_queue,
            already_processed=already_processed,
            progress_queue=progress_queue,
            save_event=save_event,
        )
        walkers.append(w)
        w.start()
    for src in config.read_paths:
        utils_dedupe.throttle_puts(walk_queue.qsize())
        walk_queue.put(src)
    walk_done.set()
    for w in walkers:
        w.join()


# duplicate finding


def _find_duplicates_and_report(  # pylint: disable=too-many-arguments
    config: "DedupeCopyConfig",
    queues: Dict[str, "queue.Queue"],
    manifest: "Manifest",
    collisions: Any,
    save_event: threading.Event,
) -> Tuple[Any, Any]:
    """Find duplicate files by comparing checksums across directories."""
    work_stop_event = threading.Event()
    result_stop_event = threading.Event()
    result_processor = ResultProcessor(
        stop_event=result_stop_event,
        result_queue=queues["result"],
        collisions=collisions,
        manifest=manifest,
        progress_queue=queues["progress"],
        config=config,
        save_event=save_event,
    )
    result_processor.start()

    result_fh = None
    if config.csv_report_path:
        # The file is opened here and closed in the finally block
        # to ensure it remains open for the duration of the function.
        result_fh = open(config.csv_report_path, "ab")
        result_fh.write(f"Src: {config.read_paths}\n".encode("utf-8"))
        result_fh.write(
            "Collision #, MD5, Path, Size (bytes), mtime\n".encode("utf-8")
        )

    try:
        if queues["progress"]:
            queues["progress"].put(
                (
                    HIGH_PRIORITY,
                    "message",
                    "Starting %s read workers",
                    config.read_threads,
                )
            )
        work_threads = []
        for _ in range(config.read_threads):
            w = ReadThread(
                work_queue=queues["work"],
                result_queue=queues["result"],
                stop_event=work_stop_event,
                progress_queue=queues["progress"],
                save_event=save_event,
            )
            work_threads.append(w)
            w.start()
        _walk_fs(
            config,
            queues,
            manifest.read_sources,
            save_event=save_event,
        )
        while not queues["work"].empty():
            if queues["progress"]:
                queues["progress"].put(
                    (
                        HIGH_PRIORITY,
                        "message",
                        "Waiting for work queue to empty: %d items remain",
                        queues["work"].qsize(),
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
                        line = f'{group}, {md5}, "{item[0]}", {item[1]}, {item[2]}\n'.encode(
                            "utf-8"
                        )
                        result_fh.write(line)
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


# copy core


def queue_copy_work(  # pylint: disable=too-many-arguments
    copy_queue: "queue.Queue[Tuple[str, str, int]]",
    data: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    copied: Any,
    ignore: Optional[List[str]] = None,
    ignore_empty_files: bool = False,
) -> Any:
    """Queue copy operations for file duplication."""
    for md5, path, mtime, size in info_parser(data):
        if md5 not in copied:
            action_required = True
            if ignore and any(
                fnmatch.fnmatch(os.path.basename(path), pattern) for pattern in ignore
            ):
                action_required = False

            if action_required:
                if not ignore_empty_files or md5 != "d41d8cd98f00b204e9800998ecf8427e":
                    copied[md5] = None
                utils_dedupe.throttle_puts(copy_queue.qsize())
                copy_queue.put((path, mtime, size))
            elif progress_queue:
                progress_queue.put((LOW_PRIORITY, "not_copied", path))
        elif progress_queue:
            progress_queue.put((LOW_PRIORITY, "not_copied", path))
    return copied


def _handle_copy_operation(  # pylint: disable=too-many-arguments
    dupes: Any,
    all_data: Any,
    config: "DedupeCopyConfig",
    path_rules_func: Optional[Callable[..., Tuple[str, str]]],
    progress_queue: "queue.PriorityQueue[Any]",
    no_copy: Any,
) -> None:
    """Queues up the copy work, waits for threads to finish"""
    stop_event = threading.Event()
    copy_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue()
    workers = []
    copied = no_copy
    if progress_queue:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Starting %s copy workers",
                config.copy_threads,
            )
        )
    for _ in range(config.copy_threads):
        c = CopyThread(
            copy_queue=copy_queue,
            config=config,
            stop_event=stop_event,
            progress_queue=progress_queue,
            path_rules_func=path_rules_func,
        )
        workers.append(c)
        c.start()
    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Copying to %r", config.copy_to_path)
        )
    # copied is passed to here so we don't try to copy "comparison" manifests
    # copied is a dict-like, so it's update in place
    queue_copy_work(
        copy_queue,
        dupes,
        progress_queue,
        copied,
        ignore=config.ignored_patterns,
        ignore_empty_files=config.keep_empty,
    )
    queue_copy_work(
        copy_queue,
        all_data,
        progress_queue,
        copied,
        ignore=config.ignored_patterns,
        ignore_empty_files=config.keep_empty,
    )
    stop_event.set()
    for c in workers:
        c.join()
    if progress_queue and copied is not None:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Processed %s unique items", len(copied))
        )


class Manifest:
    """Storage of manifest data. Presents the hash dict but tracks the
    read files in a separate structure"""

    cache_size = 10000

    def __init__(
        self,
        manifest_paths: Optional[Union[str, List[str]]],
        save_path: Optional[str] = None,
        temp_directory: Optional[str] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        self.temp_directory = temp_directory
        if save_path:
            self.path = save_path
        else:
            assert (
                temp_directory is not None
            ), "temp_directory must be provided if save_path is not"
            self.path = os.path.join(
                temp_directory, f"temporary_{random.getrandbits(16)}.dict"
            )
        self.md5_data: Any = {}
        self.read_sources: Any = {}
        self.save_event = save_event
        if manifest_paths:
            if not isinstance(manifest_paths, list):
                self.load(manifest_paths)
            else:
                self._load_manifest_list(manifest_paths)
        else:
            sources_path = f"{self.path}.read"
            # no data yet
            if os.path.exists(self.path):
                logger.info("Removing old manifest file at: %r", self.path)
                os.unlink(self.path)
            if os.path.exists(sources_path):
                logger.info("Removing old manifest sources file at: %r", sources_path)
                os.unlink(sources_path)
            logger.info("creating manifests %s / %s", self.path, sources_path)
            self.md5_data = DefaultCacheDict(
                list, db_file=self.path, max_size=self.cache_size
            )
            self.read_sources = CacheDict(
                db_file=sources_path, max_size=self.cache_size
            )

    def __contains__(self, key: str) -> bool:
        """Check if key exists in manifest."""
        return key in self.md5_data

    def __getitem__(self, key: str) -> Any:
        """Get value for key from manifest."""
        return self.md5_data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set value for key in manifest."""
        self.md5_data[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete key from manifest."""
        del self.md5_data[key]

    def __len__(self) -> int:
        """Return number of hash keys in manifest."""
        return len(self.md5_data)

    def save(
        self, path: Optional[str] = None, keys: Optional[List[str]] = None
    ) -> None:
        """Save manifest to disk at specified path."""
        if self.save_event:
            self.save_event.set()
        path = path or self.path
        try:
            self._write_manifest(path=path, keys=keys)
            self.md5_data.save(db_file=path)
            self.read_sources.save(db_file=f"{path}.read")
        finally:
            if self.save_event:
                self.save_event.clear()

    def load(self, path: Optional[str] = None) -> None:
        """Load manifest from disk at specified path."""
        path = path or self.path
        self.md5_data, self.read_sources = self._load_manifest(path=path)

    def items(self) -> Any:
        """Return items view of manifest data."""
        return self.md5_data.items()

    def iteritems(self) -> Any:
        """Deprecated: Use items() instead"""
        return self.md5_data.items()

    def _write_manifest(
        self, path: Optional[str] = None, keys: Optional[List[str]] = None
    ) -> None:
        path = path or self.path
        logger.info("Writing manifest of %s hashes to %s", len(self.md5_data), path)
        logger.info(
            "Writing sources of %s files to %s.read", len(self.read_sources), path
        )
        if not keys:
            dict_iter = self.md5_data.items()
        else:
            dict_iter = ((k, self.md5_data[k]) for k in keys)
        for _, info in dict_iter:
            for file_data in info:
                src = file_data[0]
                if src not in self.read_sources:
                    self.read_sources[src] = None

    def hash_set(self) -> Set[str]:
        """Return set of all hash keys in manifest."""
        return set(self.md5_data.keys())

    def _load_manifest(self, path: Optional[str] = None) -> Tuple[Any, Any]:
        path = path or self.path
        logger.info("Reading manifest from %r...", path)
        # Would be nice to just get the fd, but backends require a path
        md5_data = DefaultCacheDict(list, db_file=path, max_size=self.cache_size)
        logger.info("... read %s hashes", len(md5_data))
        read_sources = CacheDict(db_file=f"{path}.read", max_size=self.cache_size)
        logger.info("... in %s files", len(read_sources))
        return md5_data, read_sources

    @staticmethod
    def _combine_manifests(manifests: List[Tuple[Any, Any]]) -> Tuple[Any, Any]:
        """Combine multiple manifest data structures into one.

        Args:
            manifests: List of (md5_data, read_sources) tuples to combine

        Returns:
            Tuple of (combined_md5_data, combined_read_sources)
        """
        base, read = manifests[0]
        for m, r in manifests[1:]:
            for key, files in m.items():
                for info in files:
                    if info not in base[key]:
                        base[key].append(info)
            for key in r:
                read[key] = None
        return base, read

    def _load_manifest_list(self, manifests: List[str]) -> None:
        if not isinstance(manifests, list):
            raise TypeError("manifests must be a list")
        read_manifest_data, read_sources = self._load_manifest(manifests[0])
        loaded = [(read_manifest_data, read_sources)]
        for src in manifests[1:]:
            loaded.append(self._load_manifest(src))
        self.md5_data, self.read_sources = Manifest._combine_manifests(loaded)

    def convert_manifest_paths(
        self, paths_from: str, paths_to: str, temp_directory: Optional[str] = None
    ) -> None:
        """Replaces all prefixes for all paths in the manifest with a new prefix"""
        temp_directory = temp_directory or self.temp_directory
        for key, val in self.md5_data.items():
            new_values = []
            for file_data in val:
                new_values.append(
                    [file_data[0].replace(paths_from, paths_to, 1)] + file_data[1:]
                )
            self.md5_data[key] = new_values
        # build a new set of values and move into place
        # Note: Using CacheDict for read_sources (could optimize with a persistent set implementation)
        db_file = self.read_sources.db_file_path()
        assert temp_directory is not None, "temp_directory must be provided"
        new_sources = CacheDict(
            db_file=os.path.join(temp_directory, "temp_convert.dict"),
            max_size=self.cache_size,
        )
        for key in self.read_sources:
            new_sources[key.replace(paths_from, paths_to, 1)] = None
        del self.read_sources
        new_sources.save(db_file=db_file)
        self.read_sources = new_sources
        self.md5_data.save()
        self.read_sources.save()


def _display_summary(config: DedupeCopyConfig):
    """Displays a summary of the configuration."""
    logger.info("=" * 70)
    logger.info("DEDUPE COPY - Operation Summary")
    logger.info("=" * 70)
    if config.read_from_path:
        paths = (
            config.read_from_path
            if isinstance(config.read_from_path, list)
            else [config.read_from_path]
        )
        logger.info("Source path(s): %s path(s)", len(paths))
        for p in paths:
            logger.info("  - %s", p)
    if config.copy_to_path:
        logger.info("Destination: %s", config.copy_to_path)
    if config.manifests_in_paths:
        manifests = (
            config.manifests_in_paths
            if isinstance(config.manifests_in_paths, list)
            else [config.manifests_in_paths]
        )
        logger.info("Input manifest(s): %s manifest(s)", len(manifests))
    if config.manifest_out_path:
        logger.info("Output manifest: %s", config.manifest_out_path)
    if config.extensions:
        logger.info("Extension filter: %s", ", ".join(config.extensions))
    if config.ignored_patterns:
        logger.info("Ignored patterns: %s", ", ".join(config.ignored_patterns))
    if config.path_rules:
        logger.info("Path rules: %s", ", ".join(config.path_rules))
    logger.info(
        "Threads: walk=%s, read=%s, copy=%s",
        config.walk_threads,
        config.read_threads,
        config.copy_threads,
    )
    logger.info(
        "Options: keep_empty=%s, preserve_stat=%s, no_walk=%s",
        config.keep_empty,
        config.preserve_stat,
        config.no_walk,
    )
    if config.compare_manifests:
        comp_list = (
            config.compare_manifests
            if isinstance(config.compare_manifests, list)
            else [config.compare_manifests]
        )
        logger.info("Compare manifests: %s manifest(s)", len(comp_list))
    logger.info("=" * 70)
    logger.info("")


def run_dupe_copy(config: DedupeCopyConfig) -> None:
    """For external callers this is the entry point for dedupe + copy"""
    # Ensure logging is configured for programmatic calls
    _ensure_logging_configured()
    _display_summary(config)

    temp_directory = tempfile.mkdtemp(suffix="dedupe_copy")

    save_event = threading.Event()
    manifest = Manifest(
        config.manifests_in_paths,
        save_path=config.manifest_out_path,
        temp_directory=temp_directory,
        save_event=save_event,
    )
    compare = Manifest(
        config.compare_manifests, save_path=None, temp_directory=temp_directory
    )

    if config.no_copy:
        for item in config.no_copy:
            compare[item] = None

    if config.no_walk and not manifest:
        raise ValueError("If --no-walk is specified, a manifest must be supplied.")
    path_rules_func: Optional[Callable[..., Tuple[str, str]]] = None
    if config.path_rules:
        path_rules_func = utils_dedupe.build_path_rules(config.path_rules)
    all_stop = threading.Event()
    work_queue: "queue.Queue[str]" = queue.Queue()
    result_queue: "queue.Queue[Tuple[str, int, float, str]]" = queue.Queue()
    progress_queue: "queue.PriorityQueue[Any]" = queue.PriorityQueue()
    walk_queue: "queue.Queue[str]" = queue.Queue()
    copy_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue()
    queues = {
        "work": work_queue,
        "result": result_queue,
        "progress": progress_queue,
        "walk": walk_queue,
        "copy": copy_queue,
    }
    progress_thread = ProgressThread(
        queues,
        stop_event=all_stop,
        save_event=save_event,
    )
    progress_thread.start()
    collisions = None
    if manifest and (
        config.convert_manifest_paths_to or config.convert_manifest_paths_from
    ):
        manifest.convert_manifest_paths(
            config.convert_manifest_paths_from, config.convert_manifest_paths_to
        )

    # storage for hash collisions
    collisions_file = os.path.join(temp_directory, "collisions.db")
    collisions = DefaultCacheDict(list, db_file=collisions_file, max_size=10000)
    if manifest and not config.ignore_old_collisions:
        # rebuild collision list
        for md5, info in manifest.iteritems():
            if len(info) > 1:
                collisions[md5] = info
    if config.no_walk:
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
        dupes, all_data = _find_duplicates_and_report(
            config,
            queues,
            manifest,
            collisions,
            save_event,
        )
    total_size = utils_dedupe.extension_report(all_data)
    logger.info("Total Size of accepted: %s bytes", total_size)
    if config.manifest_out_path:
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Saving complete manifest from search")
        )
        all_data.save(path=config.manifest_out_path)
    if config.copy_to_path is not None:
        # Warning: strip dupes out of all data, this assumes dupes correctly
        # follows handling of keep_empty (not a dupe even if md5 is same for
        # zero byte files)
        for md5 in dupes:
            if md5 in all_data:
                del all_data[md5]
        # copy the duplicate files first and then ignore them for the full pass
        progress_queue.put(
            (HIGH_PRIORITY, "message", "Running copy to %r", config.copy_to_path)
        )
        _handle_copy_operation(
            dupes, all_data, config, path_rules_func, progress_queue, compare
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