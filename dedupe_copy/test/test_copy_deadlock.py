"""Test for deadlock in copy operations"""

import os
import queue
import shutil
import tempfile
import threading
import unittest

# pylint: disable=wrong-import-position
from dedupe_copy.config import CopyConfig
from dedupe_copy.threads import CopyThread
from dedupe_copy.test import utils

# pylint: enable=wrong-import-position


class TestCopyDeadlock(unittest.TestCase):
    """Test for deadlock in copy operations"""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="test_copy_deadlock_")
        self.target_dir = os.path.join(self.temp_dir, "target")
        os.makedirs(self.target_dir)
        self.source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(self.source_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def create_file(self, path: str, content: str) -> str:
        """Helper to create a file with content."""
        utils.write_file(path, seed=0, size=len(content), initial=content)
        return path

    def test_copy_thread_deadlock_on_filtered_extension(self) -> None:
        """
        Verify that CopyThread does not deadlock when an item is filtered out.

        This test simulates a scenario where files that should be skipped (due to
        extension filtering) are placed on the work queue. The bug occurs when
        the thread continues to the next item without calling `task_done()`,
        causing `queue.join()` to block indefinitely.
        """
        source_file_txt = self.create_file(
            os.path.join(self.source_dir, "source.txt"), "content"
        )
        source_file_jpg = self.create_file(
            os.path.join(self.source_dir, "source.jpg"), "photo"
        )

        work_queue: "queue.Queue[tuple[str, str, int]]" = queue.Queue()
        stop_event = threading.Event()

        copy_config = CopyConfig(
            target_path=self.target_dir,
            read_paths=[],
            extensions=[".txt"],  # Only copy .txt files
            path_rules=None,
            preserve_stat=False,
        )

        copy_thread = CopyThread(
            work_queue, stop_event, copy_config=copy_config, progress_queue=None
        )
        copy_thread.start()

        stat_txt = os.stat(source_file_txt)
        stat_jpg = os.stat(source_file_jpg)
        work_queue.put((source_file_txt, "2024_01", int(stat_txt.st_size)))
        work_queue.put(
            (source_file_jpg, "2024_01", int(stat_jpg.st_size))
        )  # This item should be skipped

        # This function will block if the deadlock occurs
        def wait_for_queue_to_finish():
            work_queue.join()
            stop_event.set()  # Signal the copy thread to stop

        join_thread = threading.Thread(target=wait_for_queue_to_finish)
        join_thread.start()

        # If the thread deadlocks, this join will time out.
        join_thread.join(timeout=5)

        self.assertFalse(
            join_thread.is_alive(), "CopyThread deadlocked on filtered item!"
        )

        # Clean up the copy thread
        if join_thread.is_alive():
            # If the thread is still alive, we have a deadlock.
            # We can't safely stop the CopyThread, but we can prevent the test from hanging.
            pass
        else:
            copy_thread.join()

        # Verify that the correct file was copied. Default is no_change.
        dest_file_txt = os.path.join(self.target_dir, "source.txt")
        self.assertTrue(
            os.path.exists(dest_file_txt),
            f"Expected file was not copied to {dest_file_txt}",
        )

        # Verify that the filtered file was NOT copied
        dest_file_jpg = os.path.join(self.target_dir, "source.jpg")
        self.assertFalse(
            os.path.exists(dest_file_jpg),
            f"Filtered file was incorrectly copied to {dest_file_jpg}",
        )
