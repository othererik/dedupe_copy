"""Thread workers for walking, hashing, copying, and progress reporting"""

import fnmatch
import logging
import os
import queue
import shutil
import threading
import time
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from .progress_worker import ProgressThread

__all__ = [
    "ProgressThread",
    "CopyThread",
    "DeleteThread",
    "ReadThread",
    "ResultProcessor",
    "WalkThread",
    "HIGH_PRIORITY",
    "LOW_PRIORITY",
]

from .config import CopyConfig, WalkConfig
from .disk_cache_dict import CacheDict, PersistentSet
from .manifest import Manifest
from .path_rules import strip_read_path_prefix
from .utils import (
    hash_file,
    lower_extension,
    match_extension,
    read_file,
)

if TYPE_CHECKING:
    from rich.progress import TaskID
    from .ui import ConsoleUI
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
    seen_paths: Optional[set] = None
    seen_lock: Optional[threading.Lock] = None


def _check_is_ignored(
    path: str,
    ignore: Optional[List[str]],
    ignore_regex: Optional[re.Pattern],
    progress_queue: Optional["queue.PriorityQueue[Any]"],
) -> bool:
    """Checks if a path should be ignored, reporting the reason if so."""
    if ignore_regex and ignore_regex.match(os.path.normcase(path)):
        if ignore and progress_queue:
            # Fallback to loop only to find specific pattern for logging
            for ignored_pattern in ignore:
                if fnmatch.fnmatch(path, ignored_pattern):
                    progress_queue.put(
                        (HIGH_PRIORITY, "ignored", path, ignored_pattern)
                    )
                    break
        return True

    if ignore:
        for ignored_pattern in ignore:
            if fnmatch.fnmatch(path, ignored_pattern):
                if progress_queue:
                    progress_queue.put(
                        (HIGH_PRIORITY, "ignored", path, ignored_pattern)
                    )
                return True
    return False


def _is_file_processing_required(
    filepath: str,
    already_processed: Any,
    ignore: Optional[List[str]],
    extensions: Optional[List[str]],
    progress_queue: Optional["queue.PriorityQueue[Any]"],
    ignore_regex: Optional[re.Pattern] = None,
    extension_matcher: Optional[Any] = None,
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
        ignore_regex: An optional compiled regex for ignore patterns.
        extension_matcher: An optional compiled extension matcher.

    Returns:
        True if the file should be processed, False otherwise.
    """
    if already_processed:
        if filepath in already_processed:
            return False
        abs_filepath = os.path.abspath(filepath)
        if abs_filepath != filepath and abs_filepath in already_processed:
            return False

    if _check_is_ignored(filepath, ignore, ignore_regex, progress_queue):
        return False

    if extension_matcher:
        if not extension_matcher.match(filepath):
            return False
    elif extensions:
        if not match_extension(extensions, filepath):
            return False
    return True


def _mark_path_seen(
    path: str,
    seen_paths: Optional[set],
    seen_lock: Optional[threading.Lock],
) -> bool:
    """Returns True if path was not previously seen (and records it), False otherwise."""
    if seen_paths is None:
        return True
    norm_p = os.path.normcase(os.path.abspath(path))
    if seen_lock is not None:
        with seen_lock:
            if norm_p in seen_paths:
                return False
            seen_paths.add(norm_p)
            return True
    if norm_p in seen_paths:
        return False
    seen_paths.add(norm_p)
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
    if _check_is_ignored(
        src,
        config.walk_config.ignore,
        config.walk_config.ignore_regex,
        config.progress_queue,
    ):
        return

    try:
        scandir_it = os.scandir(src)
    except OSError as e:
        if config.progress_queue:
            config.progress_queue.put((MEDIUM_PRIORITY, "error", src, e))
        return

    dir_count = 0
    file_count = 0
    accepted_count = 0
    last_file: Optional[str] = None
    last_accepted: Optional[str] = None

    with scandir_it as entries:
        for entry in entries:
            fn = entry.path
            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False
            if is_dir:
                dir_count += 1
                config.walk_queue.put(fn)
                continue
            file_count += 1
            last_file = fn

            if _is_file_processing_required(
                fn,
                config.already_processed,
                config.walk_config.ignore,
                config.walk_config.extensions,
                config.progress_queue,
                config.walk_config.ignore_regex,
                extension_matcher=config.walk_config.extension_matcher,
            ):
                if not _mark_path_seen(fn, config.seen_paths, config.seen_lock):
                    continue
                config.work_queue.put(fn)
                accepted_count += 1
                last_accepted = fn

    if config.progress_queue and (dir_count or file_count or accepted_count):
        config.progress_queue.put(
            (
                LOW_PRIORITY,
                "walk_batch",
                dir_count,
                file_count,
                accepted_count,
                last_file,
                last_accepted,
            )
        )


def _copy_file(
    src: str,
    dest: str,
    preserve_stat: bool,
    progress_queue: Optional["queue.PriorityQueue[Any]"],
) -> bool:
    """Helper to copy a single file. Returns True on success, False on failure."""
    dest_dir = os.path.dirname(dest)
    dest_existed = os.path.exists(dest)
    try:
        if os.path.abspath(src) == os.path.abspath(dest) or (
            dest_existed and os.path.exists(src) and os.path.samefile(src, dest)
        ):
            raise shutil.SameFileError(f"{src!r} and {dest!r} are the same file")
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
        return True
    except (OSError, IOError, shutil.Error) as e:
        if not dest_existed and os.path.exists(dest):
            try:
                os.remove(dest)
            except OSError:
                pass
        if progress_queue:
            progress_queue.put(
                (
                    MEDIUM_PRIORITY,
                    "error",
                    src,
                    f"Error copying to {repr(dest)}: {e}",
                )
            )
        return False


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
        # Get relative path from the read_path root on a strict path boundary
        rel_path, matched = strip_read_path_prefix(src, self.config.read_paths)
        if matched:
            return os.path.join(self.config.target_path, rel_path)

        # Fallback if source not under any read_path
        return os.path.join(self.config.target_path, os.path.basename(src))

    def _resolve_destination_path(self, src: str, dest: str) -> Optional[str]:
        """Resolves destination path collisions safely across worker threads."""
        # pylint: disable=protected-access
        with self.config._dest_lock:
            norm_src = os.path.normcase(os.path.abspath(src))
            norm_dest = os.path.normcase(os.path.abspath(dest))

            try:
                if norm_src == norm_dest or (
                    os.path.exists(dest)
                    and os.path.exists(src)
                    and os.path.samefile(src, dest)
                ):
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                src,
                                f"Error copying to {repr(dest)}: "
                                "Source and destination are the same file",
                            )
                        )
                    return None
            except OSError:
                pass

            claimed_by = self.config._claimed_destinations.get(norm_dest)
            is_collision = False
            if claimed_by is not None and claimed_by != norm_src:
                is_collision = True
            elif claimed_by is None and os.path.exists(dest):
                # Destination already exists on disk from prior state.
                # Check if it already has identical content to src (e.g. resumed manifest run).
                try:
                    same_content = (
                        os.path.exists(src)
                        and os.path.getsize(dest) == os.path.getsize(src)
                        and hash_file(dest) == hash_file(src)
                    )
                except OSError:
                    same_content = False
                if not same_content:
                    is_collision = True

            if not is_collision:
                self.config._claimed_destinations[norm_dest] = norm_src
                return dest

            if not self.config.rename_on_collision:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            MEDIUM_PRIORITY,
                            "error",
                            src,
                            f"Destination path collision at {repr(dest)}; "
                            "skipping copy to prevent data loss "
                            "(use --rename-on-collision to rename).",
                        )
                    )
                return None

            base, ext = os.path.splitext(dest)
            counter = 1
            while True:
                candidate = f"{base}_{counter}{ext}"
                norm_cand = os.path.normcase(os.path.abspath(candidate))
                if (
                    norm_cand not in self.config._claimed_destinations
                    and not os.path.exists(candidate)
                ):
                    self.config._claimed_destinations[norm_cand] = norm_src
                    return candidate
                counter += 1

    def _process_copy_task(self, src: str, mtime: str, size: int) -> None:
        """Process a single copy task."""
        if self.config.extension_matcher:
            if not self.config.extension_matcher.match(src):
                return
        elif not match_extension(self.config.extensions, src):
            return

        initial_dest = self._get_destination_path(src, mtime, size)
        dest = self._resolve_destination_path(src, initial_dest)
        if dest is None:
            return

        copied: Optional[bool] = True
        if not self.config.dry_run:
            copied = _copy_file(
                src, dest, self.config.preserve_stat, self.progress_queue
            )
        elif self.progress_queue:
            self.progress_queue.put((LOW_PRIORITY, "copied", src, dest))

        if copied is False:
            return

        if self.config.delete_on_copy:
            self._handle_delete_on_copy(src, dest)

    def _handle_delete_on_copy(self, src: str, dest: str) -> None:
        """Handle deletion of source file after copy."""
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
                if os.path.abspath(src) == os.path.abspath(dest) or (
                    os.path.exists(src)
                    and os.path.exists(dest)
                    and os.path.samefile(src, dest)
                ):
                    return
                os.remove(src)
                if self.progress_queue:
                    self.progress_queue.put((LOW_PRIORITY, "deleted", src))
                if self.deleted_queue:
                    self.deleted_queue.put((src, dest))
            except OSError as e:
                if self.progress_queue:
                    self.progress_queue.put((MEDIUM_PRIORITY, "error", src, str(e)))

    def run(self) -> None:
        """The main execution loop for the thread.

        This method continuously fetches tasks from the work queue and
        performs the copy operation until the stop event is set and the
        queue is empty.
        """
        while not self.stop_event.is_set() or not self.work.empty():
            try:
                src, mtime, size = self.work.get(True, 0.01)
            except queue.Empty:
                continue

            try:
                self._process_copy_task(src, mtime, size)
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

    def _merge_files_for_hash(
        self,
        md5: str,
        new_files: list[tuple[str, int, float]],
        already_existed: bool = True,
    ) -> tuple[list[tuple[str, int, float]], int]:
        """Merges new file entries for a hash while deduplicating by normalized path."""
        if not already_existed and isinstance(self.md5_data, CacheDict):
            current_files = []
        else:
            current_files = list(self.md5_data[md5])
        index_by_norm_path = {
            os.path.normcase(os.path.abspath(f[0])): idx
            for idx, f in enumerate(current_files)
        }
        added_distinct = 0
        for file_info in new_files:
            norm_p = os.path.normcase(os.path.abspath(file_info[0]))
            if norm_p in index_by_norm_path:
                current_files[index_by_norm_path[norm_p]] = file_info
            else:
                index_by_norm_path[norm_p] = len(current_files)
                current_files.append(file_info)
                added_distinct += 1
        return current_files, added_distinct

    def _record_read_sources(self, new_read_sources: list[str]) -> None:
        """Records processed file paths into manifest.read_sources."""
        if not new_read_sources or not isinstance(self.manifest, Manifest):
            return
        if isinstance(self.manifest.read_sources, PersistentSet):
            self.manifest.read_sources.update(new_read_sources)
        else:
            for src in new_read_sources:
                self.manifest.read_sources.add(src)

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

        is_manifest = isinstance(self.manifest, Manifest)
        new_read_sources: list[str] = []

        for md5, new_files in self._local_cache.items():
            try:
                already_existed = md5 in self.md5_data
                current_files, added_distinct = self._merge_files_for_hash(
                    md5, new_files, already_existed=already_existed
                )
                self.md5_data[md5] = current_files

                if is_manifest:
                    new_read_sources.extend(file_info[0] for file_info in new_files)

                is_collision = len(current_files) > 1 or (
                    already_existed and not current_files and added_distinct > 0
                )
                # If we are not de-duplicating empty files, they are never a collision.
                if not self.dedupe_empty and new_files and new_files[0][1] == 0:
                    is_collision = False

                if is_collision:
                    self.collisions[md5] = current_files
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

        self._record_read_sources(new_read_sources)
        self._local_cache.clear()
        self._batch_count = 0

    def _process_single_result(
        self, md5: str, size: int, mtime: float, src: str
    ) -> None:
        """Process a single result item."""
        try:
            if md5 not in self._local_cache:
                self._local_cache[md5] = []
            self._local_cache[md5].append((src, size, mtime))
            self._batch_count += 1

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
                md5, size, mtime, src = self.results.get(True, 0.01)
                self._process_single_result(md5, size, mtime, src)
                processed += 1
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
                src = self.work.get(True, 0.01)
                try:
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
                src = self.work.get(True, 0.01)
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
        seen_paths: Optional[set] = None,
        seen_lock: Optional[threading.Lock] = None,
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
            seen_paths: Optional shared set of already visited paths in this walk.
            seen_lock: Optional lock protecting seen_paths.
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
            seen_paths=seen_paths,
            seen_lock=seen_lock,
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
                src = self.walk_queue.get(True, 0.01)
                try:
                    if not os.path.isdir(src):
                        if not os.path.exists(src):
                            raise RuntimeError(
                                f"Directory disappeared during walk: {src!r}"
                            )
                        raise ValueError(f"Unexpected file in work queue: {src!r}")
                    if not _mark_path_seen(
                        src,
                        self.distribute_config.seen_paths,
                        self.distribute_config.seen_lock,
                    ):
                        continue
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
