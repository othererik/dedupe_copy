"""Test covering a race condition in the copy operation found in 1.1.1 release.
Will expand this test module later to cover other similar edges around the end
of operations.
"""

import os
import unittest
from functools import partial
import logging
import time
import shutil
from unittest.mock import patch

from dedupe_copy.test import utils
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.threads import LOW_PRIORITY

# Suppress logging to keep test output clean
logging.basicConfig(level=logging.CRITICAL)

# Use a partial to simplify calls to the main function
do_copy = partial(
    run_dupe_copy,
    ignore_old_collisions=True,
    walk_threads=2,
    read_threads=4,
    copy_threads=4,
    no_walk=False,
    preserve_stat=False,
)


class TestCopyRaceCondition(unittest.TestCase):
    """Test for race condition in file copying."""

    def setUp(self):
        """Create a temporary directory for test files."""
        self.temp_dir = utils.make_temp_dir("copy_race")

    def tearDown(self):
        """Remove the temporary directory."""
        utils.remove_dir(self.temp_dir)

    @patch("dedupe_copy.threads._copy_file")
    def test_copy_completes_before_exit(self, mock_copy_file):
        """
        Verify that all files are copied before the function exits.
        This test is designed to fail if the race condition exists where the
        main thread exits before copy workers are finished.
        """

        def delayed_copy(src, dest, preserve_stat, progress_queue):
            """A wrapper around the real copy logic that adds a delay."""
            time.sleep(0.02)  # Simulate slow copy operation
            dest_dir = os.path.dirname(dest)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir, exist_ok=True)
            if preserve_stat:
                shutil.copy2(src, dest)
            else:
                shutil.copyfile(src, dest)
            if progress_queue:
                progress_queue.put((LOW_PRIORITY, "copied", src, dest))

        mock_copy_file.side_effect = delayed_copy

        src_dir = os.path.join(self.temp_dir, "source")
        dest_dir = os.path.join(self.temp_dir, "destination")
        os.makedirs(src_dir, exist_ok=True)
        os.makedirs(dest_dir, exist_ok=True)

        file_count = 50
        utils.make_file_tree(
            src_dir,
            file_spec=file_count,
            file_size=1024,
            use_unique_files=True,
        )

        # Perform the copy operation
        do_copy(
            read_from_path=src_dir,
            copy_to_path=dest_dir,
        )

        # Verify that all files were copied
        copied_files = list(utils.walk_tree(dest_dir, include_dirs=False))

        self.assertEqual(
            len(copied_files),
            file_count,
            f"Expected {file_count} files, but found {len(copied_files)}. "
            "The copy operation likely terminated prematurely.",
        )


if __name__ == "__main__":
    unittest.main()
