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
    """Check if a file needs to be processed based on rules."""
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
    """Distributes files to the appropriate queues for processing."""
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
    """Copy to target_path for given extensions (all if None)"""

    def __init__(
        self,
        work_queue: "queue.Queue[Tuple[str, str, int]]",
        stop_event: threading.Event,
        *,
        copy_config: "CopyConfig",
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
    ) -> None:
        super().__init__()
        self.work = work_queue
        self.config = copy_config
        self.stop_event = stop_event
        self.progress_queue = progress_queue
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

        return os.path.join(self.config.target_path, ext, mtime, os.path.basename(src))

    def run(self) -> None:
        while not self.work.empty() or not self.stop_event.is_set():
            try:
                src, mtime, size = self.work.get(True, 0.1)
                if not match_extension(self.config.extensions, src):
                    continue
                dest = self._get_destination_path(src, mtime, size)
                _copy_file(src, dest, self.config.preserve_stat, self.progress_queue)
            except queue.Empty:
                pass


class ResultProcessor(threading.Thread):
    """Takes results of work queue and builds result data structure"""

    INCREMENTIAL_SAVE_SIZE = 50000

    def __init__(
        self,
        stop_event: threading.Event,
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        collisions: Any,
        manifest: Any,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        keep_empty: bool = False,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__()
        self.stop_event = stop_event
        self.results = result_queue
        self.collisions = collisions
        self.md5_data = manifest
        self.progress_queue = progress_queue
        self.empty = keep_empty
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        processed = 0
        while not self.results.empty() or not self.stop_event.is_set():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                md5, size, mtime, src = self.results.get(True, 0.1)
                collision = md5 in self.md5_data
                if self.empty and md5 == "d41d8cd98f00b204e9800998ecf8427e":
                    collision = False
                self.md5_data[md5].append([src, size, mtime])
                if collision:
                    self.collisions[md5] = self.md5_data[md5]
                processed += 1
            except queue.Empty:
                pass
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
            if processed > self.INCREMENTIAL_SAVE_SIZE:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            HIGH_PRIORITY,
                            "message",
                            "Hit incremental save size, will save manifest files",
                        )
                    )
                processed = 0
                try:
                    self.md5_data.save()
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                self.md5_data.path,
                                f"ERROR Saving incremental: {e}",
                            )
                        )


class ReadThread(threading.Thread):
    """Thread worker for hashing"""

    def __init__(
        self,
        work_queue: "queue.Queue[str]",
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        stop_event: threading.Event,
        *,
        progress_queue: Optional["queue.PriorityQueue[Any]"] = None,
        save_event: Optional[threading.Event] = None,
    ) -> None:
        super().__init__()
        self.work = work_queue
        self.results = result_queue
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        while not self.stop_event.is_set() or not self.work.empty():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = ""
            try:
                src = self.work.get(True, 0.1)
                try:
                    _throttle_puts(self.results.qsize())
                    self.results.put(read_file(src))
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put((MEDIUM_PRIORITY, "error", src, e))
            except queue.Empty:
                pass
            except (OSError, IOError, ValueError, TypeError) as err:
                if self.progress_queue:
                    self.progress_queue.put(
                        (MEDIUM_PRIORITY, "error", src, f"ERROR in file read: {err},")
                    )


class ProgressThread(threading.Thread):
    """All Status updates should come through here."""

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
        """Run loop that slurps items off the progress queue and dispatches the correct handler."""
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
        if self.error_count:
            logger.info("-" * 60)
        logger.info("Total errors: %d", self.error_count)
        logger.info(
            "Total elapsed time: %.1f seconds (%.1f minutes)", elapsed, elapsed / 60
        )
        logger.info("=" * 60)


class WalkThread(threading.Thread):
    """Thread that walks directory trees to discover files."""

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
        while not self.walk_queue.empty() or not self.stop_event.is_set():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = None
            try:
                src = self.walk_queue.get(True, 0.5)
                if not os.path.exists(src):
                    time.sleep(3)
                    if not os.path.exists(src):
                        raise RuntimeError(
                            f"Directory disappeared during walk: {src!r}"
                        )
                if not os.path.isdir(src):
                    raise ValueError(f"Unexpected file in work queue: {src!r}")
                distribute_work(src, self.distribute_config)
            except queue.Empty:
                pass
            except (OSError, ValueError, RuntimeError) as e:
                if self.distribute_config.progress_queue:
                    self.distribute_config.progress_queue.put(
                        (MEDIUM_PRIORITY, "error", src, e)
                    )
