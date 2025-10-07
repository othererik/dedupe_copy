"""Tests for deletion and dry-run functionality."""

import os
import unittest
from functools import partial

from dedupe_copy.test import utils
from dedupe_copy.core import run_dupe_copy

do_copy = partial(
    run_dupe_copy,
    ignore_old_collisions=False,
    walk_threads=1,
    read_threads=1,
    copy_threads=1,
    convert_manifest_paths_to="",
    convert_manifest_paths_from="",
    no_walk=False,
    preserve_stat=True,
)


class TestDelete(unittest.TestCase):
    """Test deletion and dry-run functionality."""

    def setUp(self):
        """Create temporary directory and test data."""
        self.temp_dir = utils.make_temp_dir("new_features")

    def tearDown(self):
        """Remove temporary directory and all test files."""
        utils.remove_dir(self.temp_dir)

    def test_delete_duplicates(self):
        """Test that duplicate files are deleted correctly."""
        # Create 5 unique files and 5 duplicates of one of them
        unique_files = utils.make_file_tree(self.temp_dir, file_count=5, file_size=100)
        duplicate_content_file = unique_files[0]

        for i in range(5):
            dupe_path = os.path.join(self.temp_dir, f"dupe_{i}.txt")
            with open(dupe_path, "wb") as f:
                with open(duplicate_content_file[0], "rb") as original:
                    f.write(original.read())

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 10, "Should have 10 files initially")

        # Run with --delete
        do_copy(read_from_path=self.temp_dir, delete_duplicates=True)

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(final_file_count, 5, "Should have 5 files after deletion")

    def test_delete_duplicates_dry_run(self):
        """Test that --dry-run prevents deletion of duplicate files."""
        # Create 5 unique files and 5 duplicates of one of them
        unique_files = utils.make_file_tree(self.temp_dir, file_count=5, file_size=100)
        duplicate_content_file = unique_files[0]

        for i in range(5):
            dupe_path = os.path.join(self.temp_dir, f"dupe_{i}.txt")
            with open(dupe_path, "wb") as f:
                with open(duplicate_content_file[0], "rb") as original:
                    f.write(original.read())

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 10, "Should have 10 files initially")

        # Run with --delete and --dry-run
        do_copy(read_from_path=self.temp_dir, delete_duplicates=True, dry_run=True)

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(
            final_file_count, 10, "Should have 10 files after dry-run, none deleted"
        )


if __name__ == "__main__":
    unittest.main()
