"""Progress tracking worker for dedupe_copy operation."""

import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Any, Optional, Tuple

from .utils import format_error_message

if TYPE_CHECKING:
    from rich.progress import TaskID
    from .ui import ConsoleUI

logger = logging.getLogger(__name__)

HIGH_PRIORITY = 1
MEDIUM_PRIORITY = 5
LOW_PRIORITY = 10


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
        save_event: Optional[threading.Event] = None,
        ui: Optional["ConsoleUI"] = None,
    ) -> None:
        """Initializes the ProgressThread.

        Args:
            work_queue: The queue of file paths to be processed.
            walk_queue: The queue of directories to be walked.
            result_queue: The queue of processing results.
            progress_queue: The queue for reporting progress.
            stop_event: An event to signal the thread to stop.
            save_event: An optional event to signal for saving progress.
            ui: Optional ConsoleUI instance for rich output.
        """
        super().__init__()
        self.work = work_queue
        self.walk_queue = walk_queue
        self.result_queue = result_queue
        self.progress_queue = progress_queue
        self.stop_event = stop_event
        self.ui = ui
        self.daemon = True

        self.last_accepted: Optional[str] = None
        self.file_count = 0
        self.file_count_log_interval = 1000
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

        # UI Task IDs
        self.walk_task_id: Optional["TaskID"] = None
        self.copy_task_id: Optional["TaskID"] = None
        self.delete_task_id: Optional["TaskID"] = None

        if self.ui:
            self.walk_task_id = self.ui.add_task("Walking filesystem...", total=None)

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
            if self.ui and self.walk_task_id is not None:
                self.ui.update_task(
                    "Walking filesystem...",
                    description=f"Discovered {self.file_count} files "
                    f"(dirs: {self.directory_count})",
                )
            else:
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
            if self.ui:
                if self.copy_task_id is None:
                    self.copy_task_id = self.ui.add_task("Copying files...", total=None)
                self.ui.update_task(
                    "Copying files...",
                    advance=1,
                    description=f"Copied {self.copied_count} files",
                )
            else:
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
            if self.ui:
                if self.delete_task_id is None:
                    self.delete_task_id = self.ui.add_task(
                        "Deleting files...", total=None
                    )
                self.ui.update_task(
                    "Deleting files...",
                    advance=1,
                    description=f"Deleted {self.deleted_count} files",
                )
            else:
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

    def do_log_message(self, message: str) -> None:
        """Log a generic message."""
        if self.ui:
            self.ui.log(message)
        else:
            logger.info(message)

    def do_log_set_total(self, task_type: str, total: int) -> None:
        """Set the total for a specific task type."""
        if not self.ui:
            return

        if task_type == "copy":
            if self.copy_task_id is None:
                self.copy_task_id = self.ui.add_task("Copying files...", total=total)
            else:
                self.ui.update_task("Copying files...", total=total)
        elif task_type == "delete":
            if self.delete_task_id is None:
                self.delete_task_id = self.ui.add_task("Deleting files...", total=total)
            else:
                self.ui.update_task("Deleting files...", total=total)
        elif task_type == "walk":
            if self.walk_task_id is None:
                self.walk_task_id = self.ui.add_task(
                    "Walking filesystem...", total=total
                )
            else:
                self.ui.update_task("Walking filesystem...", total=total)

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
                    if time.time() - last_update > 30:
                        logger.info("Saving...")
                        last_update = time.time()
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
            logger.info(
                "Final Summary: Processed %d files in %.2f seconds.",
                self.file_count,
                elapsed,
            )
        else:
            logger.info("Final Summary: No files processed.")

        if self.copied_count:
            logger.info("Total copied: %d", self.copied_count)
        if self.deleted_count:
            logger.info("Total deleted: %d", self.deleted_count)
        if self.error_count:
            logger.error("Total errors encountered: %d", self.error_count)
