"""Tests for deletion and dry-run functionality."""

import os
import unittest
from functools import partial

from dedupe_copy.test import utils
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.manifest import Manifest

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
        unique_files = utils.make_file_tree(self.temp_dir, file_spec=5, file_size=100)
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
        unique_files = utils.make_file_tree(self.temp_dir, file_spec=5, file_size=100)
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
            self.temp_dir, file_spec=3, file_size=200, prefix="large_"
        )
        small_files = utils.make_file_tree(
            self.temp_dir, file_spec=2, file_size=50, prefix="small_"
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

    def test_manifest_updated_after_delete(self):
        """Ensure the manifest is saved *after* deletions occur."""
        # Create 2 unique files and 2 duplicates of one of them
        unique_files = utils.make_file_tree(self.temp_dir, file_spec=2, file_size=100)
        duplicate_content_file = unique_files[0]

        for i in range(2):
            dupe_path = os.path.join(self.temp_dir, f"dupe_{i}.txt")
            with open(dupe_path, "wb") as f:
                with open(duplicate_content_file[0], "rb") as original:
                    f.write(original.read())

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 4, "Should have 4 files initially")

        manifest_path = os.path.join(self.manifest_dir, "manifest.db")

        # First run: generate manifest
        do_copy(read_from_path=self.temp_dir, manifest_out_path=manifest_path)

        # Second run: delete with the same manifest for in and out
        do_copy(
            read_from_path=self.temp_dir,
            manifests_in_paths=manifest_path,
            manifest_out_path=manifest_path,
            delete_duplicates=True,
        )

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(final_file_count, 2, "Should have 2 files after deletion")

        # Now, load the manifest and verify its contents
        manifest = Manifest(manifest_path, save_path=None, temp_directory=self.temp_dir)
        manifest_file_count = sum(len(file_list) for _, file_list in manifest.items())

        self.assertEqual(
            manifest_file_count, 2, "Manifest should contain 2 files after deletion"
        )

    def test_delete_min_size_bug(self):
        """
        Test that --min-delete-size correctly deletes large files even if the
        file kept is smaller than the threshold.
        """
        content = b"duplicate content"
        file_a_path = os.path.join(self.temp_dir, "a_file.txt")
        file_b_path = os.path.join(self.temp_dir, "b_file.txt")

        with open(file_a_path, "wb") as f:
            f.write(content)

        with open(file_b_path, "wb") as f:
            f.write(content)

        manifest_path = os.path.join(self.manifest_dir, "manifest.db")

        # Manually create a manifest that represents the bug scenario
        # One hash, two files, but with different sizes recorded. This can
        # happen if a file is modified after a manifest is created.
        manifest = Manifest(
            manifest_paths=None,
            save_path=manifest_path,
            temp_directory=self.temp_dir,
        )
        file_a_info = [file_a_path, 50, os.path.getmtime(file_a_path)]
        file_b_info = [file_b_path, 200, os.path.getmtime(file_b_path)]
        manifest["fake_hash"] = [file_a_info, file_b_info]
        manifest.save()
        manifest.close()

        # Run with --delete and a min-delete-size that is between the two file sizes
        # The bug would cause the large file to be kept, because the small file is checked
        # against the threshold and the whole group is skipped.
        # pylint: disable=redundant-keyword-arg
        do_copy(
            manifests_in_paths=manifest_path,
            no_walk=True,
            delete_duplicates=True,
            min_delete_size=100,
        )

        # a_file.txt should be kept (alphabetically first)
        # b_file.txt should be deleted (it's a duplicate and > 100 bytes)
        self.assertTrue(os.path.exists(file_a_path))
        self.assertFalse(os.path.exists(file_b_path))

    def test_delete_empty_files_not_deduped_by_default(self):
        """Test that empty files are not deleted by default."""
        # Create 10 empty files
        utils.make_file_tree(self.temp_dir, file_spec=10, file_size=0)

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 10, "Should have 10 empty files initially")

        # Run with --delete but without --dedupe-empty
        do_copy(
            read_from_path=self.temp_dir, delete_duplicates=True, dedupe_empty=False
        )

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(
            final_file_count,
            10,
            "Should still have 10 files, as empty files are not considered duplicates by default",
        )

    def test_delete_empty_files_with_dedupe_flag(self):
        """Test that empty files are deleted when --dedupe-empty is used."""
        # Create 10 empty files
        utils.make_file_tree(self.temp_dir, file_spec=10, file_size=0)

        initial_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(initial_file_count, 10, "Should have 10 empty files initially")

        # Run with --delete and --dedupe-empty
        do_copy(read_from_path=self.temp_dir, delete_duplicates=True, dedupe_empty=True)

        final_file_count = len(list(utils.walk_tree(self.temp_dir)))
        self.assertEqual(
            final_file_count,
            1,
            "Should have only 1 file remaining after deleting empty duplicates",
        )


if __name__ == "__main__":
    unittest.main()
