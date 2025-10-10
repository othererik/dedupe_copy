"""Test for a race condition in the filesystem walk."""

import os
import queue
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

from dedupe_copy.core import _walk_fs
from dedupe_copy.config import WalkConfig
from dedupe_copy import threads as dedupe_threads


class TestWalkRaceCondition(unittest.TestCase):
    """Test case for the race condition in _walk_fs."""

    def setUp(self):
        """Set up a temporary directory and a nested file structure."""
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(self.source_dir)
        self.expected_files = set()
        # Create a directory structure that requires multiple threads to process efficiently
        for i in range(4):
            sub_dir = os.path.join(self.source_dir, f"dir_{i}")
            os.makedirs(sub_dir)
            file_path = os.path.join(sub_dir, f"file_{i}.txt")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"content_{i}")
            self.expected_files.add(file_path)

    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.temp_dir)

    def test_walk_fs_race_condition_reduces_parallelism(self):
        """
        Verify that the _walk_fs race condition leads to reduced parallelism.

        This test patches `distribute_work` to track which threads are performing
        work. With the bug, one thread often processes the root directory and then
        all subdirectories, while other threads exit prematurely. This test asserts
        that at least two threads perform work, which should fail with the buggy
        implementation but pass with the fix.
        """
        work_queue = queue.Queue()
        walk_queue = queue.Queue()
        walk_config = WalkConfig(extensions=None, ignore=None, hash_algo="md5")

        thread_work_map = {}
        original_distribute = dedupe_threads.distribute_work

        def tracking_distribute_work(src, config):
            """Wrapper to track which thread is calling this function."""
            thread_id = threading.get_ident()
            thread_work_map.setdefault(thread_id, 0)
            thread_work_map[thread_id] += 1
            original_distribute(src, config)

        with patch(
            "dedupe_copy.threads.distribute_work", side_effect=tracking_distribute_work
        ):
            _walk_fs(
                read_paths=[self.source_dir],
                walk_config=walk_config,
                work_queue=work_queue,
                walk_queue=walk_queue,
                already_processed=set(),
                walk_threads=4,
            )

        found_files = set()
        while not work_queue.empty():
            found_files.add(work_queue.get())

        self.assertEqual(
            found_files, self.expected_files, "Should have found all files."
        )

        # This assertion verifies that the walk was parallel.
        # With the race condition, it's likely that only one thread will perform
        # all the directory scans, causing this test to fail.
        self.assertGreater(
            len(thread_work_map),
            1,
            f"Walk was not parallel; only {len(thread_work_map)} threads performed scans.",
        )
