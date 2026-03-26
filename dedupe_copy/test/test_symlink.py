"""Tests for handling symlinks in dedupe_copy."""

import os
import queue
import tempfile
import unittest

from dedupe_copy.config import WalkConfig
from dedupe_copy.threads import DistributeWorkConfig, distribute_work


class TestSymlinkHandling(unittest.TestCase):
    """Tests to verify symlink handling during filesystem walks."""

    def setUp(self):
        """Set up a temporary directory with a symlink loop."""
        # pylint: disable=consider-using-with
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = self.temp_dir.name

        self.dir1 = os.path.join(self.test_dir, "dir1")
        os.mkdir(self.dir1)

        # Create a file
        with open(os.path.join(self.dir1, "file.txt"), "w", encoding="utf-8") as f:
            f.write("hello")

        # Create a symlink to dir1 to simulate a loop
        self.symlink_path = os.path.join(self.dir1, "link_to_dir1")
        os.symlink(self.dir1, self.symlink_path)

        # Create a symlink to file.txt
        self.symlink_file = os.path.join(self.dir1, "link_to_file.txt")
        os.symlink(os.path.join(self.dir1, "file.txt"), self.symlink_file)

    def tearDown(self):
        """Clean up the temporary directory."""
        self.temp_dir.cleanup()

    def test_distribute_work_ignores_symlinks(self):
        """Verify that distribute_work ignores directory and file symlinks."""
        walk_config = WalkConfig()
        work_queue = queue.Queue()
        walk_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()

        config = DistributeWorkConfig(
            walk_config=walk_config,
            work_queue=work_queue,
            walk_queue=walk_queue,
            already_processed=set(),
            progress_queue=progress_queue,
        )

        # Run distribute_work on dir1
        distribute_work(self.dir1, config)

        # Verify walk_queue is empty (meaning the directory symlink was ignored)
        self.assertTrue(walk_queue.empty())

        # Verify work_queue only contains file.txt and not the file symlink
        files_added = []
        while not work_queue.empty():
            files_added.append(work_queue.get())

        self.assertEqual(len(files_added), 1)
        self.assertEqual(files_added[0], os.path.join(self.dir1, "file.txt"))

        # Verify progress_queue reports the symlinks as ignored
        ignored_items = []
        while not progress_queue.empty():
            _priority, action, *args = progress_queue.get()
            if action == "ignored" and args and args[1] == "symlink":
                ignored_items.append(args[0])

        self.assertEqual(len(ignored_items), 2)
        self.assertIn(self.symlink_path, ignored_items)
        self.assertIn(self.symlink_file, ignored_items)


if __name__ == "__main__":
    unittest.main()
