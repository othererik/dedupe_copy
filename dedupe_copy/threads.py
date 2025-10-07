"""
Worker threads for the dedupe_copy application.
"""
# pylint: disable=too-many-arguments,too-many-instance-attributes,too-few-public-methods
# pylint: disable=too-many-locals,too-many-branches,too-many-statements

import logging
import os
import queue
import shutil
import threading
import time
from typing import TYPE_CHECKING, Dict, Optional, Tuple, Callable, Any, Union

from . import utils_dedupe

if TYPE_CHECKING:
    from .dedupe_copy import DedupeCopyConfig

# For message output
HIGH_PRIORITY = 1
MEDIUM_PRIORITY = 5
LOW_PRIORITY = 10

INCREMENTIAL_SAVE_SIZE = 50000

logger = logging.getLogger(__name__)


class CopyThread(threading.Thread):
    """Copy to target_path for given extensions (all if None)"""

    def __init__(
        self,
        copy_queue: "queue.Queue[Tuple[str, str, int]]",
        config: "DedupeCopyConfig",
        stop_event: threading.Event,
        progress_queue: "queue.PriorityQueue[Any]",
        path_rules_func: Optional[Callable[..., Tuple[str, str]]],
    ) -> None:
        super().__init__()
        self.work = copy_queue
        self.config = config
        self.stop_event = stop_event
        self.progress_queue = progress_queue
        self.path_rules_func = path_rules_func
        self.daemon = True

    def run(self) -> None:
        while not self.work.empty() or not self.stop_event.is_set():
            try:
                src, mtime, size = self.work.get(True, 0.1)
                ext = utils_dedupe.lower_extension(src)
                if not ext:
                    ext = "no_extension"
                if not utils_dedupe.match_extension(self.config.extensions, src):
                    continue
                if self.path_rules_func is not None:
                    file_context = {
                        "extension": ext,
                        "mtime_str": mtime,
                        "_size": size,
                        "source_dirs": os.path.dirname(src),
                        "src": os.path.basename(src),
                        "read_paths": self.config.read_paths,
                        "copy_to_path": self.config.copy_to_path,
                    }
                    dest, dest_dir = self.path_rules_func(file_context)
                else:
                    assert self.config.copy_to_path is not None
                    dest = os.path.join(
                        self.config.copy_to_path, ext, mtime, os.path.basename(src)
                    )
                    dest_dir = os.path.dirname(dest)
                try:
                    if not os.path.exists(dest_dir):
                        try:
                            os.makedirs(dest_dir)
                        except OSError:
                            # thread race if it now exists, no issue
                            if not os.path.exists(dest_dir):
                                raise
                    if self.config.preserve_stat:
                        shutil.copy2(src, dest)
                    else:
                        shutil.copyfile(src, dest)
                    if self.progress_queue:
                        self.progress_queue.put((LOW_PRIORITY, "copied", src, dest))
                except (OSError, IOError, shutil.Error) as e:
                    if self.progress_queue:
                        self.progress_queue.put(
                            (
                                MEDIUM_PRIORITY,
                                "error",
                                src,
                                f"Error copying to {dest!r}: {e}",
                            )
                        )
            except queue.Empty:
                pass


class ResultProcessor(threading.Thread):
    """Takes results of work queue and builds result data structure"""

    def __init__(
        self,
        stop_event: threading.Event,
        result_queue: "queue.Queue[Tuple[str, int, float, str]]",
        collisions: Any,
        manifest: "Manifest",
        progress_queue: "queue.PriorityQueue[Any]",
        config: "DedupeCopyConfig",
        save_event: threading.Event,
    ) -> None:
        super().__init__()
        self.stop_event = stop_event
        self.results = result_queue
        self.collisions = collisions
        self.md5_data = manifest
        self.progress_queue = progress_queue
        self.config = config
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
                if self.config.keep_empty and md5 == "d41d8cd98f00b204e9800998ecf8427e":
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
            if processed > INCREMENTIAL_SAVE_SIZE:
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
        progress_queue: "queue.PriorityQueue[Any]",
        save_event: threading.Event,
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
                    utils_dedupe.throttle_puts(self.results.qsize())
                    self.results.put(utils_dedupe.read_file(src))
                except (OSError, IOError) as e:
                    if self.progress_queue:
                        self.progress_queue.put((MEDIUM_PRIORITY, "error", src, e))
            except queue.Empty:
                pass
            except (OSError, IOError, ValueError, TypeError) as err:
                if self.progress_queue:
                    self.progress_queue.put(
                        (
                            MEDIUM_PRIORITY,
                            "error",
                            src,
                            f"ERROR in file read: {err},",
                        )
                    )


class ProgressThread(threading.Thread):
    """All Status updates should come through here.
    Can process raw messages as well as message of the type:
        message, msg
        file, path
        accepted, path
        ignored, path, reason
        error, path, reason
        copied, src, dest
        not_copied, path
    """

    file_count_log_interval = 1000

    def __init__(
        self,
        queues: Dict[str, queue.Queue],
        stop_event: threading.Event,
        save_event: threading.Event,
    ) -> None:

        super().__init__()
        self.work = queues["work"]
        self.result_queue = queues["result"]
        self.progress_queue = queues["progress"]
        self.walk_queue = queues["walk"]
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
        # Performance tracking
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
            logger.info(
                "Discovered %s files (dirs: %s), accepted %s. Rate: %.1f files/sec",
                self.file_count,
                self.directory_count,
                self.accepted_count,
                files_per_sec,
            )
            logger.info(
                "Work queue has %s items. Progress queue has %s items. Walk queue has %s items.",
                self.work.qsize(),
                self.progress_queue.qsize(),
                self.walk_queue.qsize(),
            )
            logger.info("Current file: %r (last accepted: %r)", path, self.last_accepted)

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
                "Copied %s items. Skipped %s items. Rate: %.1f files/sec",
                self.copied_count,
                self.not_copied_count,
                copy_rate,
            )
            logger.info("Last file: %r -> %r", src, dest)
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

    def do_log_error(self, path: str, reason: Union[str, Exception]) -> None:
        """Log files that caused errors during processing."""
        self.error_count += 1
        error_msg = utils_dedupe.format_error_message(path, reason)
        logger.error(error_msg)

    @staticmethod
    def do_log_message(message: str, *args: Any) -> None:
        """Log a generic message."""
        logger.info(message, *args)

    def run(self) -> None:
        """Run loop that slurps items off the progress queue and dispatches
        the correct handler
        """
        last_update = time.time()
        while not self.stop_event.is_set() or not self.progress_queue.empty():
            try:
                # we should only be getting directories on this queue
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
                    work_size = self.work.qsize()
                    result_size = self.result_queue.qsize()
                    progress_size = self.progress_queue.qsize()
                    walk_size = self.walk_queue.qsize()

                    logger.debug(
                        "Status:  Work Queue Size: %s. Result Queue Size: %s. "
                        "Progress Queue Size: %s. Walk Queue Size: %s.",
                        work_size,
                        result_size,
                        progress_size,
                        walk_size,
                    )

                    # Warn about large queue sizes (potential memory issues)
                    if work_size > 40000:
                        logger.warning(
                            "Work queue is large (%d items). "
                            "Consider reducing thread counts to avoid memory issues.",
                            work_size,
                        )
                    if progress_size > 10000:
                        logger.warning(
                            "Progress queue is backing up (%d items). "
                            "This may indicate slow processing.",
                            progress_size,
                        )
            except (AttributeError, ValueError) as e:
                logger.error("Failed in progress thread: %s", e)

        # Final summary with timing and rates
        elapsed = time.time() - self.start_time
        if self.file_count:
            logger.info("=" * 60)
            logger.info("RESULTS FROM WALK:")
            logger.info("Total files discovered: %s", self.file_count)
            logger.info("Total accepted: %s", self.accepted_count)
            logger.info("Total ignored: %s", self.ignored_count)
            files_per_sec = self.file_count / elapsed if elapsed > 0 else 0
            logger.info("Average discovery rate: %.1f files/sec", files_per_sec)
        if self.copied_count:
            logger.info("-" * 60)
            logger.info("RESULTS FROM COPY:")
            logger.info("Total copied: %s", self.copied_count)
            logger.info("Total skipped: %s", self.not_copied_count)
            copy_rate = self.copied_count / elapsed if elapsed > 0 else 0
            logger.info("Average copy rate: %.1f files/sec", copy_rate)
        if self.error_count:
            logger.info("-" * 60)
        logger.info("Total errors: %s", self.error_count)
        logger.info(
            "Total elapsed time: %.1f seconds (%.1f minutes)", elapsed, elapsed / 60
        )
        logger.info("=" * 60)


class WalkThread(threading.Thread):
    """Thread that walks directory trees to discover files."""

    def __init__(
        self,
        config: "DedupeCopyConfig",
        walk_queue: "queue.Queue[str]",
        work_queue: "queue.Queue[str]",
        already_processed: Any,
        progress_queue: "queue.PriorityQueue[Any]",
        stop_event: threading.Event,
        save_event: threading.Event,
    ) -> None:
        super().__init__()
        self.config = config
        self.walk = walk_queue
        self.work = work_queue
        self.already_processed = already_processed
        self.progress = progress_queue
        self.stop_event = stop_event
        self.save_event = save_event
        self.daemon = True

    def run(self) -> None:
        while not self.walk.empty() or not self.stop_event.is_set():
            if self.save_event and self.save_event.is_set():
                time.sleep(1)
                continue
            src = None
            try:
                # we should only be getting directories on this queue
                src = self.walk.get(True, 0.5)
                if not os.path.exists(src):
                    # are we dealing with a network path, retry after sleeping
                    time.sleep(3)
                    if not os.path.exists(src):
                        raise RuntimeError(
                            f"Directory disappeared during walk: {src!r}"
                        )
                if not os.path.isdir(src):
                    raise ValueError(f"Unexpected file in work queue: {src!r}")
                # process the files, sending items off to be read and getting
                # new directories put onto the queue we read from
                utils_dedupe.distribute_work(
                    src,
                    self.already_processed,
                    self.config,
                    progress_queue=self.progress,
                    work_queue=self.work,
                    walk_queue=self.walk,
                )
            except queue.Empty:
                pass
            except (OSError, ValueError, RuntimeError) as e:
                if self.progress:
                    self.progress.put((MEDIUM_PRIORITY, "error", src, e))