"""Contains all the threading classes for dedupe_copy
These are the workers for walking, hashing, copying, and progress reporting
"""

import fnmatch
import logging
import os
import queue
import shutil
import threading
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from .config import CopyConfig, WalkConfig
from .manifest import Manifest
from .utils import (
    _throttle_puts,
    format_error_message,
    lower_extension,
    match_extension,
    read_file,
)

# For message output
HIGH_PRIORITY = 1
MEDIUM_PRIORITY = 5
LOW_PRIORITY = 10

logger = logging.getLogger(__name__)


@dataclass
class DistributeWorkConfig:
    """Configuration for the distribute_work function."""

    already_processed: Any
    walk_config: "WalkConfig"
    progress_queue: Optional["queue.PriorityQueue[Any]"]
    work_queue: "queue.Queue[str]"
    walk_queue: "queue.Queue[str]"


def _is_file_processing_required(
    filepath: str,
    already_processed: Any,
    ignore: Optional[List[str]],
    extensions: Optional[List[str]],
    progress_queue: Optional["queue.PriorityQueue[Any]"],
) -> bool:
    """Determines if a file should be processed based on various criteria.

    This function checks if a file has already been processed, if it matches
    any ignored patterns, or if it has a permitted extension.

    Args:
        filepath: The path to the file to check.
        already_processed: A set-like object of paths that have already
                           been processed.
        ignore: A list of glob patterns for files to ignore.
        extensions: A list of allowed file extensions.
        progress_queue: An optional queue for reporting progress.

    Returns:
        True if the file should be processed, False otherwise.
    """
    if filepath in already_processed:
        return False
    if ignore:
        for ignored_pattern in ignore:
            if fnmatch.fnmatch(filepath, ignored_pattern):
                if progress_queue:
                    progress_queue.put(
                        (HIGH_PRIORITY, "ignored", filepath, ignored_pattern)
                    )
                return False
    if extensions:
        if not match_extension(extensions, filepath):
            return False
    return True


def distribute_work(src: str, config: DistributeWorkConfig) -> None:
    """Scans a directory and distributes its contents to worker queues.

    This function iterates through the items in a given directory. Subdirectories
    are added to the walk queue for further scanning, and files that meet the
    processing criteria are added to the work queue.

    Args:
        src: The directory path to scan.
        config: The configuration for the work distribution.
    """
    if config.walk_config.ignore:
        for ignored_pattern in config.walk_config.ignore:
            if fnmatch.fnmatch(src, ignored_pattern):
                if config.progress_queue:
                    config.progress_queue.put(
                        (HIGH_PRIORITY, "ignored", src, ignored_pattern)
                    )
                return
    for item in os.listdir(src):
        fn = os.path.join(src, item)
        if os.path.isdir(fn):
            if config.progress_queue:
                config.progress_queue.put((LOW_PRIORITY, "dir", fn))
            _throttle_puts(config.walk_queue.qsize())
            config.walk_queue.put(fn)
            continue
        if config.progress_queue:
            config.progress_queue.put((LOW_PRIORITY, "file", fn))

        if _is_file_processing_required(
            fn,
            config.already_processed,
            config.walk_config.ignore,
            config.walk_config.extensions,
            config.progress_queue,
        ):
            _throttle_puts(config.work_queue.qsize())
            config.work_queue.put(fn)
            if config.progress_queue:
                config.progress_queue.put((HIGH_PRIORITY, "accepted", fn))


def _copy_file(
    src: str,
    dest: str,
    preserve_stat: bool,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
) -> None:
    """Helper to copy a single file."""
    dest_dir = os.path.dirname(dest)
    try:
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError:
                if not os.path.exists(dest_dir):
                    raise
        if preserve_stat:
            shutil.copy2(src, dest)
        else:
            shutil.copyfile(src, dest)
        if progress_queue:
            progress_queue.put((LOW_PRIORITY, "copied", src, dest))
    except (OSError, IOError, shutil.Error) as e:
        if progress_queue:
            progress_queue.put(
                (
                    MEDIUM_PRIORITY,
                    "error",
                    src,
                    f"Error copying to {repr(dest)}: {e}",
                )
            )


class CopyThread(threading.Thread):
    """A worker thread for copying files.

    This thread processes file copy tasks from a queue, calculating the
    destination path based on configured rules and performing the copy
    operation.

    Attributes:
        work: The queue of files to be copied.
        config: The configuration for the copy operation.
        stop_event: An event to signal the thread to stop.
        progress_queue: An optional queue for reporting progress.
    """

    def __init__(
        self,
        work_queue: "queue.Queue[Tuple[str, str, int]]",
        stop_event: threading.Event,
        *,
        copy_config: "CopyConfig",
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        deleted_queue: Optional["queue.Queue[Tuple[str, str]]"] = None,
    ) -> None:
        """Initializes the CopyThread.

        Args:
            work_queue: The queue of files to be copied.
            stop_event: An event to signal the thread to stop.
            copy_config: The configuration for the copy operation.
            progress_queue: An optional queue for reporting progress.
            deleted_queue: An optional queue to record deleted source files.
        """
        super().__init__()
        self.work = work_queue
        self.config = copy_config
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.deleted_queue = deleted_queue
        self.daemon = True

    def _get_destination_path(self, src: str, mtime: str, size: int) -> str:
        """Calculates the destination path for a file."""
        ext = lower_extension(src) or "no_extension"
        if self.config.path_rules:
            source_dirs = os.path.dirname(src)
            dest, _ = self.config.path_rules(
                self.config.target_path,
                ext,
                mtime,
                size,
                source_dirs=source_dirs,
                src=os.path.basename(src),
                read_paths=self.config.read_paths,
            )
            return dest

        # Default behavior: preserve original directory structure (no_change)
        # Get relative path from the read_path root
        for read_path in self.config.read_paths:
            if src.startswith(read_path):
                rel_path = os.path.relpath(src, read_path)
                return os.path.join(self.config.target_path, rel_path)

        # Fallback if source not under any read_path
        return os.path.join(self.config.target_path, os.path.basename(src))

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches tasks from the work queue and
        performs the copy operation until the stop event is set and the
        queue is empty.
        """
        # there is a lot going on in this loop, refactor
        # pylint: disable=R1702, R0912
        while not self.stop_event.is_set() or not self.work.empty():
            try:
                src, mtime, size = self.work.get(True, 0.1)
            except queue.Empty:
                continue

            try:
                if not match_extension(self.config.extensions, src):
                    continue

                dest = self._get_destination_path(src, mtime, size)
                if not self.config.dry_run:
                    _copy_file(
                        src, dest, self.config.preserve_stat, self.progress_queue
                    )
                elif self.progress_queue:
                    self.progress_queue.put((LOW_PRIORITY, "copied", src, dest))

                if self.config.delete_on_copy:
                    if self.config.dry_run:
                        if self.progress_queue:
                            self.progress_queue.put(
                                (
                                    HIGH_PRIORITY,
                                    "message",
                                    f"[DRY RUN] Would delete source file {src}",
                                )
                            )
                    else:
                        try:
                            os.remove(src)
                            if self.progress_queue:
                                self.progress_queue.put((LOW_PRIORITY, "deleted", src))
                            if self.deleted_queue:
                                self.deleted_queue.put((src, dest))
                        except OSError as e:
                            if self.progress_queue:
                                self.progress_queue.put(
                                    (MEDIUM_PRIORITY, "error", src, e)
                                )
            finally:
                self.work.task_done()


class ResultProcessor(threading.Thread):
    """A worker thread for processing file hashing results.

    This thread consumes results from the result queue, updates the main
    manifest, and identifies hash collisions. It processes results in batches
    for efficiency and supports incremental saving of the manifest.

    Attributes:
        stop_event: An event to signal the thread to stop.
        results: The queue of file hashing results to process.
        collisions: A dictionary-like object to store hash collisions.
        manifest: The main manifest object.
        progress_queue: An optional queue for reporting progress.
        empty: If True, empty files are processed.
        save_event: An optional event to coordinate save operations.
    """

    INCREMENTAL_SAVE_SIZE = 50000
    BATCH_SIZE = 1000

    def __init__(
        self,
        stop_event: threading.Event,
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        collisions: Any,
        manifest: Any,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        dedupe_empty: bool = False,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        """Initializes the ResultProcessor.

        Args:
            stop_event: An event to signal the thread to stop.
            result_queue: The queue of file hashing results.
            collisions: A dictionary-like object for storing collisions.
            manifest: The main manifest object.
            progress_queue: An optional queue for reporting progress.
            dedupe_empty: If True, empty files are treated as duplicates.
            save_event: An optional event to coordinate save operations.
        """
        super().__init__()

        self.stop_event = stop_event
        self.results = result_queue
        self.collisions = collisions
        self.manifest = manifest
        # Handle cases where a raw DefaultCacheDict is passed for testing
        if isinstance(manifest, Manifest):
            self.md5_data = self.manifest.md5_data
        else:
            self.md5_data = manifest
        self.progress_queue = progress_queue
        self.dedupe_empty = dedupe_empty
        self.save_event = save_event
        self.daemon = True
        self._local_cache: dict[str, list[tuple[str, int, float]]] = {}
        self._batch_count = 0

    def _commit_batch(self) -> None:
        """Commits the local cache to the main manifest."""
        if not self._local_cache:
            return

        if self.progress_queue:
            self.progress_queue.put(
                (
                    HIGH_PRIORITY,
                    "message",
                    f"Committing batch of {len(self._local_cache)} hashes.",
                )
            )

        for md5, new_files in self._local_cache.items():
            try:
                # A collision exists if the hash is already in the manifest,
                # OR if we are adding more than one file with this hash in the current batch.
                already_existed = md5 in self.md5_data
                is_collision = already_existed or (len(new_files) > 1)

                # If we are not de-duplicating empty files, they are never a collision.
                if not self.dedupe_empty and new_files and new_files[0][1] == 0:
                    is_collision = False

                # Efficiently update the manifest
                current_files = self.md5_data[md5]
                current_files.extend(new_files)
                self.md5_data[md5] = current_files

                # Add the new file paths to read_sources as well
                if isinstance(self.manifest, Manifest):
                    for file_info in new_files:
                        self.manifest.read_sources[file_info[0]] = None

                if is_collision:
                    self.collisions[md5] = self.md5_data[md5]
            except (KeyError, ValueError, TypeError) as err:
                if self.progress_queue:
                    # In case of an error, we might have multiple files for one hash
                    for file_info in new_files:
                        src = file_info[0]
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                src,
                                f"ERROR in result processing: {err}",
                            )
                        )

        self._local_cache.clear()
        self._batch_count = 0

    # pylint: disable=R0912
    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches results from the results queue,
        processes them in batches, and triggers incremental saves of the
        manifest as needed.
        """
        processed = 0
        # this code is getting complex, refactor
        while not self.stop_event.is_set() or not self.results.empty():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                md5, size, mtime, src = self.results.get(True, 0.1)
                try:
                    if md5 not in self._local_cache:
                        self._local_cache[md5] = []
                    self._local_cache[md5].append((src, size, mtime))
                    self._batch_count += 1
                    processed += 1

                    if self._batch_count >= self.BATCH_SIZE:
                        self._commit_batch()

                except (KeyError, ValueError, TypeError) as err:
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                src,
                                f"ERROR in result processing: {err}",
                            )
                        )
                finally:
                    self.results.task_done()
            except queue.Empty:
                pass

            if processed > self.INCREMENTAL_SAVE_SIZE:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            HIGH_PRIORITY,
                            "message",
                            "Hit incremental save size, will save manifest files",
                        )
                    )
                self._commit_batch()  # Commit any remaining items before saving
                processed = 0
                try:
                    if isinstance(self.manifest, Manifest):
                        self.manifest.save(rebuild_sources=False)
                    else:
                        self.manifest.save()
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        db_path = ""
                        if hasattr(self.manifest, "db_file_path"):
                            db_path = self.manifest.db_file_path()
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                db_path,
                                f"ERROR Saving incremental: {e}",
                            )
                        )
        # Commit any final items
        self._commit_batch()


class ReadThread(threading.Thread):
    """A worker thread for reading and hashing files.

    This thread consumes file paths from a work queue, reads the file content,
    calculates its hash, and places the result in a result queue.

    Attributes:
        work: The queue of file paths to be processed.
        results: The queue where hashing results are placed.
        stop_event: An event to signal the thread to stop.
        walk_config: Configuration for the file walk, including the hash algorithm.
        progress_queue: An optional queue for reporting progress.
        save_event: An optional event to coordinate save operations.
    """

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        stop_event: threading.Event,
        *,
        walk_config: "WalkConfig",
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        """Initializes the ReadThread.

        Args:
            work_queue: The queue of file paths to be processed.
            result_queue: The queue for hashing results.
            stop_event: An event to signal the thread to stop.
            walk_config: Configuration for the file walk.
            progress_queue: An optional queue for reporting progress.
            save_event: An optional event to coordinate save operations.
        """
        super().__init__()
        self.work = work_queue
        self.results = result_queue
        self.stop_event = stop_event
        self.walk_config = walk_config
        self.progress_queue = progress_queue
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches file paths from the work queue,
        hashes them, and places the results in the result queue, until the
        stop event is set and the queue is empty.
        """
        while not self.stop_event.is_set() or not self.work.empty():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                src = self.work.get(True, 0.1)
                try:
                    _throttle_puts(self.results.qsize())
                    self.results.put(
                        read_file(src, hash_algo=self.walk_config.hash_algo)
                    )
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put((MEDIUM_PRIORITY, "error", src, e))
                finally:
                    self.work.task_done()
            except queue.Empty:
                pass
            except (OSError, IOError, ValueError, TypeError) as err:
                if self.progress_queue:
                    self.progress_queue.put(
                        (MEDIUM_PRIORITY, "error", src, f"ERROR in file read: {err},")
                    )


class ProgressThread(threading.Thread):
    """A thread for monitoring and reporting the application's progress.

    This thread consumes progress messages from a priority queue and logs
    them to provide real-time feedback on the application's status. It also
    tracks various statistics and prints a summary at the end of the
    operation.

    Attributes:
        work: The work queue, monitored for size.
        result_queue: The result queue, monitored for size.
        progress_queue: The queue of progress messages to be processed.
        walk_queue: The walk queue, monitored for size.
        stop_event: An event to signal the thread to stop.
        save_event: An event to coordinate save operations.
    """

    file_count_log_interval = 1000

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        progress_queue: "queue.PriorityQueue[Any]",
        *,
        walk_queue: "queue.Queue[str]",
        stop_event: threading.Event,
        save_event: threading.Event,
    ) -> None:
        """Initializes the ProgressThread.

        Args:
            work_queue: The work queue to monitor.
            result_queue: The result queue to monitor.
            progress_queue: The queue of progress messages.
            walk_queue: The walk queue to monitor.
            stop_event: An event to signal the thread to stop.
            save_event: An event to coordinate save operations.
        """
        super().__init__()
        self.work = work_queue
        self.result_queue = result_queue
        self.progress_queue = progress_queue
        self.walk_queue = walk_queue
        self.stop_event = stop_event
        self.daemon = True
        self.last_accepted: Optional[str] = None
        self.file_count = 0
        self.directory_count = 0
        self.accepted_count = 0
        self.ignored_count = 0
        self.error_count = 0
        self.copied_count = 0
        self.not_copied_count = 0
        self.deleted_count = 0
        self.not_deleted_count = 0
        self.last_copied: Optional[str] = None
        self.save_event = save_event
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.bytes_processed = 0

    def do_log_dir(self, _path: str) -> None:
        """Log directory processing."""
        self.directory_count += 1

    def do_log_file(self, path: str) -> None:
        """Log file discovery and progress."""
        self.file_count += 1
        if self.file_count % self.file_count_log_interval == 0 or self.file_count == 1:
            elapsed = time.time() - self.start_time
            files_per_sec = self.file_count / elapsed if elapsed > 0 else 0
            message = (
                f"Discovered {self.file_count} files (dirs: {self.directory_count}), "
                f"accepted {self.accepted_count}. Rate: {files_per_sec:.1f} files/sec\n"
                f"Work queue has {self.work.qsize()} items. "
                f"Progress queue has {self.progress_queue.qsize()} items. "
                f"Walk queue has {self.walk_queue.qsize()} items.\n"
                f"Current file: {repr(path)} (last accepted: {repr(self.last_accepted)})"
            )
            logger.info(message)

    def do_log_copied(self, src: str, dest: str) -> None:
        """Log successful file copy operations."""
        self.copied_count += 1
        if (
            self.copied_count % self.file_count_log_interval == 0
            or self.copied_count == 1
        ):
            elapsed = time.time() - self.start_time
            copy_rate = self.copied_count / elapsed if elapsed > 0 else 0
            logger.info(
                "Copied %d items. Skipped %d items. Rate: %.1f files/sec\n"
                "Last file: %r -> %r",
                self.copied_count,
                self.not_copied_count,
                copy_rate,
                src,
                dest,
            )
        self.last_copied = src

    def do_log_not_copied(self, _path: str) -> None:
        """Log files that were not copied."""
        self.not_copied_count += 1

    def do_log_queued_for_delete(self, item: str) -> None:
        """Log that an item was queued for deletion."""
        logger.debug("Queued for deletion: %s", item)

    def do_log_deleted(self, _path: str) -> None:
        """Log successful file deletion."""
        self.deleted_count += 1
        if (
            self.deleted_count % self.file_count_log_interval == 0
            or self.deleted_count == 1
        ):
            elapsed = time.time() - self.start_time
            delete_rate = self.deleted_count / elapsed if elapsed > 0 else 0
            logger.info(
                "Deleted %d items. Rate: %.1f files/sec",
                self.deleted_count,
                delete_rate,
            )

    def do_log_not_deleted(self, _path: str) -> None:
        """Log files that were not deleted."""
        self.not_deleted_count += 1

    def do_log_accepted(self, path: str) -> None:
        """Log files that were accepted for processing."""
        self.accepted_count += 1
        self.last_accepted = path

    def do_log_ignored(self, path: str, reason: str) -> None:
        """Log files that were ignored during processing."""
        self.ignored_count += 1
        logger.info("Ignoring %r for %r", path, reason)

    def do_log_error(self, path: str, reason: Exception) -> None:
        """Log files that caused errors during processing."""
        self.error_count += 1
        error_msg = format_error_message(path, reason)
        logger.error(error_msg)

    @staticmethod
    def do_log_message(message: str) -> None:
        """Log a generic message."""
        logger.info(message)

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches messages from the progress queue and
        dispatches them to the appropriate handler function. It also logs
        periodic status updates.
        """
        last_update = time.time()
        while not self.stop_event.is_set() or not self.progress_queue.empty():
            try:
                item = self.progress_queue.get(True, 0.1)[1:]
                method_name = f"do_log_{item[0]}"
                method = getattr(self, method_name)
                method(*item[1:])
                last_update = time.time()
            except queue.Empty:
                if self.save_event and self.save_event.is_set():
                    logger.info("Saving...")
                    time.sleep(1)
                if time.time() - last_update > 60:
                    last_update = time.time()
                    logger.debug(
                        "Status: WorkQ: %d, ResultQ: %d, ProgressQ: %d, WalkQ: %d",
                        self.work.qsize(),
                        self.result_queue.qsize(),
                        self.progress_queue.qsize(),
                        self.walk_queue.qsize(),
                    )
            except (AttributeError, ValueError) as e:
                logger.error("Failed in progress thread: %s", e)
        self.log_final_summary()

    def log_final_summary(self) -> None:
        """Logs a final summary of the operation."""
        elapsed = time.time() - self.start_time
        if self.file_count:
            logger.info("=" * 60)
            logger.info("RESULTS FROM WALK:")
            logger.info("Total files discovered: %d", self.file_count)
            logger.info("Total accepted: %d", self.accepted_count)
            logger.info("Total ignored: %d", self.ignored_count)
            files_per_sec = self.file_count / elapsed if elapsed > 0 else 0
            logger.info("Average discovery rate: %.1f files/sec", files_per_sec)
        if self.copied_count:
            logger.info("-" * 60)
            logger.info("RESULTS FROM COPY:")
            logger.info("Total copied: %d", self.copied_count)
            logger.info("Total skipped: %d", self.not_copied_count)
            copy_rate = self.copied_count / elapsed if elapsed > 0 else 0
            logger.info("Average copy rate: %.1f files/sec", copy_rate)
        if self.deleted_count:
            logger.info("-" * 60)
            logger.info("RESULTS FROM DELETE:")
            logger.info("Total deleted: %d", self.deleted_count)
            delete_rate = self.deleted_count / elapsed if elapsed > 0 else 0
            logger.info("Average delete rate: %.1f files/sec", delete_rate)
        if self.error_count:
            logger.info("-" * 60)
        logger.info("Total errors: %d", self.error_count)
        logger.info(
            "Total elapsed time: %.1f seconds (%.1f minutes)", elapsed, elapsed / 60
        )
        logger.info("=" * 60)


class DeleteThread(threading.Thread):
    """A worker thread for deleting files.

    This thread consumes file paths from a queue and deletes them from the
    filesystem. It supports a dry-run mode for simulating deletions.

    Attributes:
        work: The queue of file paths to be deleted.
        stop_event: An event to signal the thread to stop.
        progress_queue: An optional queue for reporting progress.
        dry_run: If True, deletions are simulated but not performed.
    """

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        stop_event: threading.Event,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        deleted_queue: Optional["queue.Queue[str]"] = None,
        dry_run: bool = False,
    ) -> None:
        """Initializes the DeleteThread.

        Args:
            work_queue: The queue of file paths to be deleted.
            stop_event: An event to signal the thread to stop.
            progress_queue: An optional queue for reporting progress.
            deleted_queue: An optional queue to record successfully deleted files.
            dry_run: If True, simulates deletions.
        """
        super().__init__()
        self.work = work_queue
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.deleted_queue = deleted_queue
        self.dry_run = dry_run
        self.daemon = True

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches file paths from the work queue and
        deletes them, until the stop event is set and the queue is empty.
        """
        # pylint: disable=R1702
        while not self.stop_event.is_set() or not self.work.empty():
            try:
                src = self.work.get(True, 0.1)
                try:
                    if self.dry_run:
                        if self.progress_queue:
                            self.progress_queue.put(
                                (
                                    HIGH_PRIORITY,
                                    "message",
                                    f"[DRY RUN] Would delete {src}",
                                )
                            )
                    else:
                        try:
                            os.remove(src)
                            if self.progress_queue:
                                self.progress_queue.put((LOW_PRIORITY, "deleted", src))
                            if self.deleted_queue:
                                self.deleted_queue.put(src)
                        except OSError as e:
                            if self.progress_queue:
                                self.progress_queue.put(
                                    (MEDIUM_PRIORITY, "error", src, e)
                                )
                finally:
                    self.work.task_done()
            except queue.Empty:
                pass


class WalkThread(threading.Thread):
    """A worker thread for walking directory trees to discover files.

    This thread consumes directory paths from a walk queue, scans them for
    subdirectories and files, and distributes them to the appropriate queues
    for further processing.

    Attributes:
        walk_queue: The queue of directory paths to be walked.
        stop_event: An event to signal the thread to stop.
        distribute_config: The configuration for work distribution.
        save_event: An optional event to coordinate save operations.
    """

    def __init__(
        self,
        walk_queue: "queue.Queue[str]",
        stop_event: threading.Event,
        *,
        walk_config: "WalkConfig",
        work_queue: "queue.Queue[str]",
        already_processed: Any,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        """Initializes the WalkThread.

        Args:
            walk_queue: The queue of directory paths to be walked.
            stop_event: An event to signal the thread to stop.
            walk_config: The configuration for the filesystem walk.
            work_queue: The queue for files to be processed.
            already_processed: A set-like object of already processed paths.
            progress_queue: An optional queue for reporting progress.
            save_event: An optional event to coordinate save operations.
        """
        super().__init__()
        self.walk_queue = walk_queue
        self.stop_event = stop_event
        self.distribute_config = DistributeWorkConfig(
            already_processed=already_processed,
            walk_config=walk_config,
            progress_queue=progress_queue,
            work_queue=work_queue,
            walk_queue=walk_queue,
        )
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches directory paths from the walk queue
        and processes them, until the stop event is set and the queue is
        empty.
        """
        while not self.stop_event.is_set() or not self.walk_queue.empty():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = None
            try:
                src = self.walk_queue.get(True, 0.5)
                try:
                    if not os.path.exists(src):
                        time.sleep(3)
                        if not os.path.exists(src):
                            raise RuntimeError(
                                f"Directory disappeared during walk: {src!r}"
                            )
                    if not os.path.isdir(src):
                        raise ValueError(f"Unexpected file in work queue: {src!r}")
                    distribute_work(src, self.distribute_config)
                finally:
                    self.walk_queue.task_done()
            except queue.Empty:
                pass
            except (OSError, ValueError, RuntimeError) as e:
                if self.distribute_config.progress_queue:
                    self.distribute_config.progress_queue.put(
                        (MEDIUM_PRIORITY, "error", src, e)
                    )
