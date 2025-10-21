"""Core application logic for dedupe_copy"""

import datetime
import fnmatch
import logging
import os
import queue
import shutil
import tempfile
import threading
from collections import Counter
from typing import Any, Callable, Iterator, List, Literal, Optional, Tuple, Union

from .config import CopyConfig, CopyJob, WalkConfig, DeleteJob
from .disk_cache_dict import DefaultCacheDict
from .manifest import Manifest
from .path_rules import build_path_rules
from .threads import (
    HIGH_PRIORITY,
    LOW_PRIORITY,
    CopyThread,
    DeleteThread,
    ProgressThread,
    ReadThread,
    ResultProcessor,
    WalkThread,
)
from .utils import _throttle_puts, ensure_logging_configured, lower_extension

logger = logging.getLogger(__name__)


# pylint: disable=too-many-lines
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
    """Initializes and manages worker threads for walking the filesystem.

    Args:
        read_paths: A list of starting paths for the filesystem walk.
        walk_config: Configuration for the walk, including file extensions and ignored patterns.
        work_queue: A queue to which discovered file paths are added for processing.
        walk_queue: A queue for managing the directories to be walked.
        already_processed: A set-like object of paths to skip.
        progress_queue: An optional queue for reporting progress.
        walk_threads: The number of concurrent threads to use for walking.
        save_event: An optional event to signal for saving progress.
    """
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
    walk_queue.join()
    walk_done.set()
    for w in walkers:
        w.join()


def _extension_report(md5_data: Any, show_count: int = 10) -> int:
    """Generates and logs a report of file extensions, sorted by size and count.

    Args:
        md5_data: A dictionary containing file hash information and metadata.
        show_count: The number of top extensions to display in the report.

    Returns:
        The total size in bytes of all files accounted for in the report.
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


def generate_report(
    csv_report_path: str,
    collisions: Any,
    read_paths: Literal[""] | list[str] | None,
    hash_algo: str,
) -> None:
    """Creates a CSV file detailing all detected file collisions.

    Args:
        csv_report_path: The file path where the report will be saved.
        collisions: A dictionary of hash collisions to be included in the report.
        read_paths: The source paths that were scanned for duplicates.
        hash_algo: The hashing algorithm used to detect duplicates.
    """
    logger.info("Generating CSV report at %s", csv_report_path)
    try:
        with open(csv_report_path, "wb") as result_fh:
            if read_paths:
                result_fh.write(f"Src: {read_paths}\n".encode("utf-8"))
            result_fh.write(
                f"Collision #, {hash_algo.upper()}, Path, Size (bytes), mtime\n".encode(
                    "utf-8"
                )
            )
            if collisions:
                group = 0
                for md5, info in collisions.items():
                    group += 1
                    for item in info:
                        line = (
                            f"{group}, {md5}, {repr(item[0])}, {item[1]}, {item[2]}\n"
                        )
                        result_fh.write(line.encode("utf-8"))
    except IOError as e:
        logger.error("Could not write report to %s: %s", csv_report_path, e)


def _start_read_threads_and_process_results(
    work_queue: "queue.Queue[str]",
    result_queue: "queue.Queue[Tuple[str, int, float, str]]",
    collisions: Any,
    manifest: Any,
    dedupe_empty: bool,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    save_event: Optional[threading.Event],
    read_threads: int,
    walk_config: "WalkConfig",
) -> Tuple[ResultProcessor, List[ReadThread], threading.Event, threading.Event]:
    """Configures and starts the file reading and result processing threads.

    Args:
        work_queue: Queue of file paths to be read and hashed.
        result_queue: Queue for storing the results of file processing.
        collisions: A dictionary-like object to store detected collisions.
        manifest: The main manifest object for tracking all file data.
        dedupe_empty: If True, empty files are treated as duplicates.
        progress_queue: Optional queue for reporting progress updates.
        save_event: Optional event to signal when to save the manifest.
        read_threads: The number of concurrent threads for reading files.
        walk_config: Configuration for the filesystem walk.

    Returns:
        A tuple containing the result processor, a list of read threads,
        and events to signal work and result processing completion.
    """
    work_stop_event = threading.Event()
    result_stop_event = threading.Event()
    result_processor = ResultProcessor(
        result_stop_event,
        result_queue,
        collisions,
        manifest,
        dedupe_empty=dedupe_empty,
        progress_queue=progress_queue,
        save_event=save_event,
    )
    result_processor.start()
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
            walk_config=walk_config,
            progress_queue=progress_queue,
            save_event=save_event,
        )
        work_threads.append(w)
        w.start()
    return result_processor, work_threads, work_stop_event, result_stop_event


# pylint: disable=too-many-branches
def find_duplicates(
    read_paths: List[str],
    work_queue: "queue.Queue[str]",
    result_queue: "queue.Queue[Tuple[str, int, float, str]]",
    manifest: Any,
    collisions: Any,
    *,
    walk_config: WalkConfig,
    progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    walk_threads: int = 4,
    read_threads: int = 8,
    save_event: Optional[threading.Event] = None,
    walk_queue: Optional["queue.Queue[str]"] = None,
) -> Tuple[Any, Any]:
    """Coordinates the process of finding duplicate files.

    This function orchestrates the filesystem walk, file reading, and result
    processing to identify duplicate files based on their content hashes.

    Args:
        read_paths: A list of directory paths to scan for files.
        work_queue: The queue for file paths awaiting processing.
        result_queue: The queue for storing results from file hashing.
        manifest: The central manifest of all processed files.
        collisions: A dictionary to store detected hash collisions.
        walk_config: Configuration for the filesystem walk.
        progress_queue: Optional queue for progress updates.
        walk_threads: Number of threads for walking the filesystem.
        read_threads: Number of threads for reading and hashing files.
        dedupe_empty: If True, empty files are included in the processing.
        save_event: Event to signal when to save the manifest.
        walk_queue: Optional queue for managing directories to be walked.

    Returns:
        A tuple containing the dictionary of collisions and the final manifest.
    """
    (
        result_processor,
        work_threads,
        work_stop_event,
        result_stop_event,
    ) = _start_read_threads_and_process_results(
        work_queue,
        result_queue,
        collisions,
        manifest,
        walk_config.dedupe_empty,
        progress_queue,
        save_event,
        read_threads,
        walk_config,
    )
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
    work_queue.join()
    work_stop_event.set()
    for worker in work_threads:
        worker.join()
    result_queue.join()
    result_stop_event.set()
    result_processor.join()
    if collisions:
        logger.info("Hash Collisions:")
        for md5, info in collisions.items():
            logger.info("  %s: %s", walk_config.hash_algo.upper(), md5)
            for item in info:
                logger.info("    %r, %s", item[0], item[1])
    else:
        logger.info("No Duplicates Found")
    return collisions, manifest


def info_parser(data: Any) -> Iterator[Tuple[str, str, str, int]]:
    """Parses manifest data and yields structured file information.

    This generator function iterates through a dictionary of file hashes and
    associated metadata, yielding a tuple with the hash, path, formatted
    modification time, and size for each file.

    Args:
        data: A dictionary where keys are file hashes and values are lists of
              file metadata tuples (path, size, mtime).

    Yields:
        A tuple containing the MD5 hash, file path, a string representation
        of the modification year and month, and the file size.
    """
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


def copy_data(
    all_data: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    *,
    copy_job: "CopyJob",
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Manages the process of copying files and deleting source files.

    This function sets up and manages worker threads to perform file copy
    and delete operations. It queues copy tasks and, if configured, also
    queues duplicate files from the source for deletion.

    Args:
        all_data: A dictionary of all files to be considered for copying.
        progress_queue: An optional queue for reporting progress.
        copy_job: The configuration for the copy operation.

    Returns:
        A tuple containing a list of all deleted source paths and a list
        of (source, destination) path tuples for moved files.
    """
    # pylint: disable=too-many-statements
    copy_stop_event = threading.Event()
    copy_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue()
    # This queue holds (source, dest) tuples of files deleted by CopyThreads
    deleted_after_copy_queue: "queue.Queue[Tuple[str, str]]" = queue.Queue()
    # This queue holds source paths to be deleted because they are dupes of --compare
    delete_only_queue: "queue.Queue[str]" = queue.Queue()
    copy_workers = []

    # Create a set of hashes that should not be copied.
    # This includes hashes from the compare manifest and hashes we've already
    # decided to copy in this run. This set is modified in this function.
    no_copy_hashes = None
    if copy_job.no_copy:
        no_copy_hashes = copy_job.no_copy.hash_set()
    hashes_to_skip = set()
    if no_copy_hashes:
        hashes_to_skip.update(no_copy_hashes)

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
            copy_stop_event,
            copy_config=copy_job.copy_config,
            progress_queue=progress_queue,
            deleted_queue=deleted_after_copy_queue,
        )
        copy_workers.append(c)
        c.start()
    if progress_queue:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                f"Copying to {copy_job.copy_config.target_path}",
            )
        )

    # --- Start of inlined queue_copy_work logic ---
    for md5, path, mtime, size in info_parser(all_data):
        if md5 not in hashes_to_skip:
            action_required = True
            if copy_job.ignore:
                for ignored_pattern in copy_job.ignore:
                    if fnmatch.fnmatch(path, ignored_pattern):
                        action_required = False
                        break
            if action_required:
                if not (size == 0 and not copy_job.dedupe_empty):
                    # Add hash to skip set so other files with the same hash are not copied
                    hashes_to_skip.add(md5)
                _throttle_puts(copy_queue.qsize())
                copy_queue.put((path, mtime, size))
            elif progress_queue:
                progress_queue.put((LOW_PRIORITY, "not_copied", path))
        elif copy_job.delete_on_copy and delete_only_queue is not None:
            # If a file's hash is in our skip set (either from compare manifest
            # or from a file we've already queued for copying), and we are in "move"
            # mode, delete this duplicate from the source.
            _throttle_puts(delete_only_queue.qsize())
            delete_only_queue.put(path)
            if progress_queue:
                progress_queue.put((LOW_PRIORITY, "queued_for_delete", path))
        elif progress_queue:
            progress_queue.put((LOW_PRIORITY, "not_copied", path))
    # --- End of inlined queue_copy_work logic ---

    # Wait for all tasks to be processed by the workers
    copy_queue.join()

    # Signal copy threads to stop and wait for them to terminate
    copy_stop_event.set()
    for c in copy_workers:
        c.join()

    # Now, handle the deletion of files that were duplicates of the compare manifest
    all_deleted_files = []
    moved_files = []
    while not deleted_after_copy_queue.empty():
        try:
            src, dest = deleted_after_copy_queue.get_nowait()
            all_deleted_files.append(src)
            moved_files.append((src, dest))
        except queue.Empty:
            break

    if not delete_only_queue.empty():
        delete_stop_event = threading.Event()
        delete_workers = []
        # This queue holds files deleted by DeleteThreads
        deleted_dupes_queue: "queue.Queue[str]" = queue.Queue()
        if progress_queue:
            progress_queue.put(
                (
                    HIGH_PRIORITY,
                    "message",
                    f"Starting deletion of {delete_only_queue.qsize()} source files "
                    "that are duplicates of the compare manifest.",
                )
            )
        for _ in range(copy_job.copy_threads):  # Re-use copy_threads for deletion
            d = DeleteThread(
                delete_only_queue,
                delete_stop_event,
                progress_queue=progress_queue,
                deleted_queue=deleted_dupes_queue,
                dry_run=copy_job.dry_run,
            )
            delete_workers.append(d)
            d.start()

        delete_only_queue.join()
        delete_stop_event.set()
        for d in delete_workers:
            d.join()

        while not deleted_dupes_queue.empty():
            try:
                all_deleted_files.append(deleted_dupes_queue.get_nowait())
            except queue.Empty:
                break

    if progress_queue:
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Processed {len(hashes_to_skip)} unique items")
        )
        if all_deleted_files:
            progress_queue.put(
                (
                    HIGH_PRIORITY,
                    "message",
                    f"Deleted a total of {len(all_deleted_files)} files from source.",
                )
            )
    return all_deleted_files, moved_files


def delete_files(
    duplicates: Any,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    *,
    delete_job: "DeleteJob",
    hashes_to_delete_all: Optional[set] = None,
) -> List[str]:
    """Coordinates the deletion of duplicate files.

    This function identifies which files to delete from a list of duplicates,
    queues them for deletion, and manages a pool of worker threads to carry
    out the deletion tasks. It now returns only the paths of files that were
    successfully deleted.

    Args:
        duplicates: A dictionary where keys are hashes and values are lists of
                    file metadata for duplicate files.
        progress_queue: An optional queue for reporting progress.
        delete_job: The configuration for the delete operation.
        hashes_to_delete_all: A set of hashes for which all associated files
                              should be deleted, overriding the default behavior
                              of keeping one.

    Returns:
        A list of paths of the files that were actually deleted.
    """
    stop_event = threading.Event()
    delete_queue: "queue.Queue[str]" = queue.Queue()
    deleted_queue: "queue.Queue[str]" = queue.Queue()
    workers = []
    files_to_delete_count = 0
    files_to_delete = []
    if hashes_to_delete_all is None:
        hashes_to_delete_all = set()

    for _hash, file_list in duplicates.items():
        if not file_list:
            continue

        sorted_file_list = sorted(file_list, key=lambda x: x[0])
        files_to_process = []

        if _hash in hashes_to_delete_all:
            files_to_process.extend(sorted_file_list)
        elif len(sorted_file_list) > 1:
            # Default behavior: keep the first file, queue the rest for deletion
            files_to_process.extend(sorted_file_list[1:])

        for file_info in files_to_process:
            path_to_delete, size, _ = file_info
            if size == 0 and not delete_job.dedupe_empty:
                if progress_queue:
                    message = (
                        f"Skipping deletion of empty file {path_to_delete} "
                        "because --dedupe-empty is not set."
                    )
                    progress_queue.put(
                        (
                            LOW_PRIORITY,
                            "message",
                            message,
                        )
                    )
                continue

            if size >= delete_job.min_delete_size_bytes:
                files_to_delete.append(path_to_delete)
                files_to_delete_count += 1
            elif progress_queue:
                message = (
                    f"Skipping deletion of {path_to_delete} with size {size} bytes "
                    f"(smaller than threshold {delete_job.min_delete_size_bytes})."
                )
                progress_queue.put(
                    (
                        LOW_PRIORITY,
                        "message",
                        message,
                    )
                )

    if progress_queue:
        if delete_job.dry_run:
            logger.info("DRY RUN: Would have deleted %d files.", files_to_delete_count)
            progress_queue.put(
                (
                    HIGH_PRIORITY,
                    "message",
                    f"DRY RUN: Would have started deletion of {files_to_delete_count} files.",
                )
            )
        else:
            progress_queue.put(
                (
                    HIGH_PRIORITY,
                    "message",
                    f"Starting deletion of {files_to_delete_count} files.",
                )
            )
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                f"Starting {delete_job.delete_threads} delete workers",
            )
        )

    for _ in range(delete_job.delete_threads):
        d = DeleteThread(
            delete_queue,
            stop_event,
            progress_queue=progress_queue,
            deleted_queue=deleted_queue,
            dry_run=delete_job.dry_run,
        )
        workers.append(d)
        d.start()

    for path_to_delete in files_to_delete:
        _throttle_puts(delete_queue.qsize())
        delete_queue.put(path_to_delete)

    delete_queue.join()

    stop_event.set()
    for d in workers:
        d.join()

    successfully_deleted_files = []
    while not deleted_queue.empty():
        try:
            successfully_deleted_files.append(deleted_queue.get_nowait())
        except queue.Empty:
            break
    return successfully_deleted_files


def verify_manifest_fs(manifest: Manifest) -> bool:
    """
    Verifies that files in the manifest exist and their sizes match.

    Args:
        manifest: The Manifest object to verify.

    Returns:
        True if all files are verified successfully, False otherwise.
    """
    logger.info("Starting manifest verification for %d hashes...", len(manifest))
    verified_count = 0
    error_count = 0

    for _, file_list in manifest.items():
        for file_path, expected_size, _ in file_list:
            try:
                if not os.path.exists(file_path):
                    logger.error("VERIFY FAILED: File not found: %s", file_path)
                    error_count += 1
                    continue

                actual_size = os.path.getsize(file_path)
                if actual_size != expected_size:
                    logger.error(
                        "VERIFY FAILED: Size mismatch for %s: expected %d, got %d",
                        file_path,
                        expected_size,
                        actual_size,
                    )
                    error_count += 1
                else:
                    verified_count += 1
            except OSError as e:
                logger.error("VERIFY FAILED: Could not access %s: %s", file_path, e)
                error_count += 1

    total_files = verified_count + error_count
    logger.info(
        "Verification complete. Total files checked: %d. Verified: %d. Errors: %d.",
        total_files,
        verified_count,
        error_count,
    )

    if error_count == 0:
        logger.info("Manifest verification successful.")
        return True

    logger.error("Manifest verification failed.")
    return False


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
    hash_algo: str = "md5",
    convert_manifest_paths_to: str = "",
    convert_manifest_paths_from: str = "",
    no_walk: bool = False,
    no_copy: Optional[List[str]] = None,
    dedupe_empty: bool = False,
    compare_manifests: Optional[Union[str, List[str]]] = None,
    preserve_stat: bool = False,
    delete_duplicates: bool = False,
    delete_on_copy: bool = False,
    dry_run: bool = False,
    min_delete_size: int = 0,
    verify_manifest: bool = False,
) -> None:
    """Main entry point for the deduplication and copy functionality.

    This function serves as the primary interface for external callers,
    orchestrating the entire workflow of finding, reporting, and handling
    duplicate files based on the provided configuration.

    Args:
        read_from_path: The source path(s) to scan for files.
        extensions: A list of file extensions to include in the scan.
        manifests_in_paths: Path(s) to input manifest files.
        manifest_out_path: Path to save the output manifest file.
        path_rules: Rules for path conversion during the copy process.
        copy_to_path: The destination path for copied files.
        ignore_old_collisions: If True, ignores collisions from old manifests.
        ignored_patterns: Patterns for files/directories to ignore.
        csv_report_path: Path to generate a CSV report of duplicates.
        walk_threads: Number of threads for walking the filesystem.
        read_threads: Number of threads for reading and hashing files.
        copy_threads: Number of threads for copying files.
        hash_algo: The hashing algorithm to use.
        convert_manifest_paths_to: Target path for manifest path conversion.
        convert_manifest_paths_from: Source path for manifest path conversion.
        no_walk: If True, skips the filesystem walk and uses manifests only.
        no_copy: A list of hashes to exclude from copying.
        dedupe_empty: If True, empty files are processed.
        compare_manifests: Manifests to compare against for filtering copies.
        preserve_stat: If True, preserves file stats during copy.
        delete_duplicates: If True, deletes duplicate files.
        dry_run: If True, simulates deletion without actual file removal.
        min_delete_size: Minimum size for a file to be considered for deletion.
        verify_manifest: If True, verifies the integrity of the manifest.
    """
    # Ensure logging is configured for programmatic calls
    ensure_logging_configured()

    # On a dry run, we never want to create or modify a manifest on disk.
    if dry_run:
        manifest_out_path = None

    # Argument validation
    if manifests_in_paths and manifest_out_path:
        # Check if any of the input manifests are the same as the output manifest
        if any(
            os.path.abspath(p) == os.path.abspath(manifest_out_path)
            for p in manifests_in_paths
        ):
            raise ValueError(
                "Input manifest path cannot be the same as the output manifest path."
            )

    if compare_manifests and manifest_out_path:
        # Check if any of the compare manifests are the same as the output manifest
        if any(
            os.path.abspath(p) == os.path.abspath(manifest_out_path)
            for p in compare_manifests
        ):
            raise ValueError(
                "Compare manifest path cannot be the same as the output manifest path."
            )

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
        for p in manifests:
            logger.info("  - %s", p)
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
        "Options: dedupe_empty=%s, preserve_stat=%s, no_walk=%s",
        dedupe_empty,
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
        for p in comp_list:
            logger.info("  - %s", p)
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

    if verify_manifest:
        verify_manifest_fs(manifest)
        manifest.close()
        try:
            shutil.rmtree(temp_directory)
        except OSError as err:
            logger.warning(
                "Failed to cleanup the temp_directory: %s with err: %s",
                temp_directory,
                err,
            )
        return

    if no_copy:
        for item in no_copy:
            compare[item] = None

    if no_walk:
        if not manifests_in_paths:
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
        for md5, info in manifest.items():
            if len(info) > 1:
                collisions[md5] = info
    walk_config = WalkConfig(
        extensions=extensions,
        ignore=ignored_patterns,
        hash_algo=hash_algo,
        dedupe_empty=dedupe_empty,
    )

    if no_walk:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Not walking file system. Using stored manifests",
            )
        )
        # Rebuild collision list from the manifest
        if manifest:
            logger.info("Manifest loaded with %d items.", len(manifest))
            for md5, info in manifest.items():
                if len(info) > 1:
                    collisions[md5] = info
            logger.info("Found %d collisions in manifest.", len(collisions))
        dupes = collisions
        all_data = manifest
    else:
        progress_queue.put(
            (
                HIGH_PRIORITY,
                "message",
                "Running the duplicate search, generating reports",
            )
        )
        dupes, all_data = find_duplicates(
            read_from_path or [],
            work_queue,
            result_queue,
            manifest,
            collisions,
            walk_config=walk_config,
            progress_queue=progress_queue,
            walk_threads=walk_threads,
            read_threads=read_threads,
            save_event=save_event,
            walk_queue=walk_queue,
        )
    work_queue.join()
    result_queue.join()
    total_size = _extension_report(all_data)
    logger.info("Total Size of accepted: %s bytes", total_size)
    if csv_report_path:
        generate_report(
            csv_report_path=csv_report_path,
            collisions=dupes,
            read_paths=read_from_path,
            hash_algo=hash_algo,
        )
    if delete_duplicates:
        if copy_to_path:
            logger.error("Cannot use --delete and --copy-path at the same time.")
        else:
            delete_job = DeleteJob(
                delete_threads=copy_threads,
                dry_run=dry_run,
                min_delete_size_bytes=min_delete_size,
                dedupe_empty=dedupe_empty,
            )

            # If comparing, we want to delete ALL files that match the compare manifest's hashes.
            hashes_to_delete_all = set(compare.md5_data) if compare else None

            # When using --compare with --delete, we need to consider all files,
            # not just those with internal duplicates, for deletion.
            data_to_scan_for_deletes = all_data if compare else dupes

            deleted_files = delete_files(
                data_to_scan_for_deletes,
                progress_queue,
                delete_job=delete_job,
                hashes_to_delete_all=hashes_to_delete_all,
            )
            # Update the manifest with the deleted files
            if manifest_out_path and not dry_run:
                # Update the manifest with the deleted files
                if deleted_files:
                    all_data.remove_files(deleted_files)
                progress_queue.put(
                    (HIGH_PRIORITY, "message", "Saving updated manifest after deletion")
                )
                all_data.save(path=manifest_out_path, no_walk=True)
    elif copy_to_path is not None:
        # copy the duplicate files first and then ignore them for the full pass
        progress_queue.put(
            (HIGH_PRIORITY, "message", f"Running copy to {repr(copy_to_path)}")
        )

        effective_read_paths = read_from_path or []
        if not isinstance(effective_read_paths, list):
            effective_read_paths = [effective_read_paths]

        if no_walk and not effective_read_paths and manifest:
            manifest_paths = list(manifest.read_sources.keys())
            if manifest_paths:
                # Use commonpath to determine the most likely source root.
                common_base = os.path.commonpath(manifest_paths)
                # If commonpath returns a file (e.g., only one file in manifest), get its dir.
                if common_base and not os.path.isdir(common_base):
                    common_base = os.path.dirname(common_base)

                if common_base:
                    logger.info(
                        "No source path provided with --no-walk; using common base path "
                        "from manifest to preserve structure: %s",
                        common_base,
                    )
                    effective_read_paths = [common_base]

        copy_config = CopyConfig(
            target_path=copy_to_path,
            read_paths=effective_read_paths,
            extensions=extensions,
            path_rules=path_rules_func,
            preserve_stat=preserve_stat,
            delete_on_copy=delete_on_copy,
            dry_run=dry_run,
        )
        copy_job = CopyJob(
            copy_config=copy_config,
            ignore=ignored_patterns,
            no_copy=compare,
            dedupe_empty=dedupe_empty,
            copy_threads=copy_threads,
            delete_on_copy=delete_on_copy,
            dry_run=dry_run,
        )
        deleted_files, moved_files = copy_data(
            all_data,
            progress_queue,
            copy_job=copy_job,
        )

        # This logic handles manifest updates for both moved files (delete-on-copy)
        # and files that were only deleted (due to --compare).
        if moved_files or deleted_files:
            # First, handle the path updates for files that were moved.
            # This operation removes the old source path and adds the new destination path.
            if moved_files:
                all_data.update_paths(moved_files)

            # Next, handle the removal of files that were deleted but not moved.
            # These are files that were duplicates of the --compare manifest.
            # We must not try to re-process files that were already handled by update_paths.
            # Convert to sets for robust duplicate handling and efficient subtraction.
            moved_source_paths = {src for src, _ in moved_files}
            all_deleted_paths = set(deleted_files)

            files_to_remove_only = list(all_deleted_paths - moved_source_paths)

            if files_to_remove_only:
                all_data.remove_files(files_to_remove_only)

        if manifest_out_path and not dry_run:
            progress_queue.put(
                (HIGH_PRIORITY, "message", "Saving complete manifest after copy")
            )
            all_data.save(path=manifest_out_path, no_walk=True)
    else:
        # If not deleting or copying, save the manifest if a path is provided
        if manifest_out_path and not dry_run:
            progress_queue.put(
                (HIGH_PRIORITY, "message", "Saving complete manifest from search")
            )
            all_data.save(path=manifest_out_path, no_walk=no_walk)
    all_stop.set()
    while progress_thread.is_alive():
        progress_thread.join(5)
    manifest.close()
    compare.close()
    collisions.close()
    try:
        shutil.rmtree(temp_directory)
    except OSError as err:
        logger.warning(
            "Failed to cleanup the collisions file: %s with err: %s",
            collisions_file,
            err,
        )
