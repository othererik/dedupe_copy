import os
import unittest
from functools import partial
import logging
import time

from dedupe_copy.test import utils
from dedupe_copy.core import run_dupe_copy

# Suppress logging to keep test output clean
logging.basicConfig(level=logging.CRITICAL)

# Use a partial to simplify calls to the main function
do_copy = partial(
    run_dupe_copy,
    ignore_old_collisions=True,
    walk_threads=2,
    read_threads=4,
    copy_threads=16,
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

    def test_copy_completes_before_exit(self):
        """
        Verify that all files are copied before the function exits.
        This test is designed to fail if the race condition exists where the
        main thread exits before copy workers are finished.
        """
        for i in range(5):  # Run the test multiple times to increase chance of failure
            with self.subTest(i=i):
                src_dir = os.path.join(self.temp_dir, f"source_{i}")
                dest_dir = os.path.join(self.temp_dir, f"destination_{i}")
                os.makedirs(src_dir, exist_ok=True)
                os.makedirs(dest_dir, exist_ok=True)

                # A moderate number of files to keep the test reasonably fast
                file_count = 200
                utils.make_file_tree(
                    src_dir,
                    file_count=file_count,
                    file_size=1024,  # Larger files to ensure unique content
                    use_unique_files=True,
                )

                # Add a small delay to increase chances of race condition
                # This is a bit of a hack, but can help expose timing issues
                time.sleep(0.1)

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
                    f"Subtest {i}: Expected {file_count} files, but found {len(copied_files)}. "
                    "The copy operation likely terminated prematurely."
                )

if __name__ == "__main__":
    unittest.main()