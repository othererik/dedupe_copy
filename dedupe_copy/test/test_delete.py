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
        self.temp_dir = utils.make_temp_dir("test_data")
        self.manifest_dir = utils.make_temp_dir("manifest")

    def tearDown(self):
        """Remove temporary directory and all test files."""
        utils.remove_dir(self.temp_dir)
        utils.remove_dir(self.manifest_dir)

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

    def test_delete_no_walk_with_size_threshold(self):
        """Test deletion with --no-walk and a size threshold."""
        # Create 5 unique files: 3 large, 2 small
        large_files = utils.make_file_tree(
            self.temp_dir, file_count=3, file_size=200, prefix="large_"
        )
        small_files = utils.make_file_tree(
            self.temp_dir, file_count=2, file_size=50, prefix="small_"
        )

        # Create duplicates: 2 for a large file, 1 for a small file
        large_dupe_content_file = large_files[0]
        for i in range(2):
            dupe_path = os.path.join(self.temp_dir, f"large_dupe_{i}.txt")
            with open(dupe_path, "wb") as f:
                with open(large_dupe_content_file[0], "rb") as original:
                    f.write(original.read())

        small_dupe_content_file = small_files[0]
        dupe_path = os.path.join(self.temp_dir, "small_dupe_0.txt")
        with open(dupe_path, "wb") as f:
            with open(small_dupe_content_file[0], "rb") as original:
                f.write(original.read())

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 8, "Should have 8 files initially")

        # First run: generate manifest
        manifest_path = os.path.join(self.manifest_dir, "manifest.db")
        do_copy(read_from_path=self.temp_dir, manifest_out_path=manifest_path)

        # Second run: delete with --no-walk and size threshold
        run_dupe_copy(
            ignore_old_collisions=False,
            walk_threads=1,
            read_threads=1,
            copy_threads=1,
            convert_manifest_paths_to="",
            convert_manifest_paths_from="",
            preserve_stat=True,
            manifests_in_paths=manifest_path,
            no_walk=True,
            delete_duplicates=True,
            min_delete_size=100,
        )

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        # Expected: 3 large files (1 original + 2 dupes, 2 deleted) -> 1
        #           2 small files (1 original + 1 dupe, 0 deleted) -> 2
        #           2 other large files -> 2
        #           1 other small file -> 1
        # Total: 1 + 2 + 2 + 1 = 6
        self.assertEqual(
            final_file_count, 6, "Should have 6 files after selective deletion"
        )

    def test_delete_is_deterministic(self):
        """Test that file deletion is deterministic, preserving the first path alphabetically."""
        # Create directories in a non-alphabetical order to ensure test validity
        os.makedirs(os.path.join(self.temp_dir, "b"))
        os.makedirs(os.path.join(self.temp_dir, "a"))
        os.makedirs(os.path.join(self.temp_dir, "c"))

        file_paths = [
            os.path.join(self.temp_dir, "b", "dup.txt"),
            os.path.join(self.temp_dir, "a", "dup.txt"),
            os.path.join(self.temp_dir, "c", "dup.txt"),
        ]

        # Create identical files
        for path in file_paths:
            with open(path, "w", encoding="utf-8") as f:
                f.write("identical content")

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 3, "Should have 3 files initially")

        # Run with --delete
        do_copy(read_from_path=self.temp_dir, delete_duplicates=True)

        remaining_files = list(utils.walk_tree(self.temp_dir))
        self.assertEqual(len(remaining_files), 1, "Should have 1 file after deletion")

        # The file with the lexicographically first path should be preserved
        preserved_file = os.path.join(self.temp_dir, "a", "dup.txt")
        self.assertEqual(remaining_files[0], preserved_file)


if __name__ == "__main__":
    unittest.main()
