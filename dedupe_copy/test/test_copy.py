"""Basic tests around copy operations"""

from functools import partial
import os
import unittest

from dedupe_copy.test import utils

from dedupe_copy import dedupe_copy


do_copy = partial(
    dedupe_copy.run_dupe_copy,
    ignore_old_collisions=False,
    walk_threads=4,
    read_threads=8,
    copy_threads=8,
    convert_manifest_paths_to="",
    convert_manifest_paths_from="",
    no_walk=False,
    preserve_stat=True,
)


class TestCopySystem(
    unittest.TestCase
):  # pylint: disable=attribute-defined-outside-init
    """Test system level copy of files"""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("copy_sys")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    def test_copy_no_change_no_dupes(self):
        """Test copying of small tree to same structure - no dupe no change"""
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_count=10, extensions=None, file_size=1000
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify the copied data
        for file_info in self.file_data:
            file_info[0] = file_info[0].replace(self.temp_dir, copy_to_path, 1)
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_copy_no_change_no_dupes_no_rules(self):
        """Test copying of small tree to same structure - no dupes no rules"""
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_count=10, extensions=None, file_size=1000
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir, copy_to_path=copy_to_path, path_rules=None
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify the copied data
        for file_info in self.file_data:
            osrc = file_info[0]
            file_info[0] = file_info[0].replace(self.temp_dir, copy_to_path, 1)
            # the default is currently extension/mtime
            file_info[0] = utils.apply_path_rules(
                osrc, file_info[0], self.temp_dir, copy_to_path, ["extension", "mtime"]
            )
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_copy_dupe_zero_byte_reject_empty_dupes(self):
        """Small tree to same structure with all zero byte files. 0 is dupe.
        Only one should remain.
        """
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_count=10, extensions=None, file_size=0
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            keep_empty=False,
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify we copied only one file:
        files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(len(files), 1, f"Did not copy just 1 file: {files}")
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_copy_dupe_zero_byte_copy_empty_dupes(self):
        """Small tree to same structure with all zero byte files. 0 not dupe.
        All should be copied.
        """
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_count=10, extensions=None, file_size=0
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            keep_empty=True,
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify the copied data
        for file_info in self.file_data:
            file_info[0] = file_info[0].replace(self.temp_dir, copy_to_path, 1)
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Failed to copy files: {notes}")


if __name__ == "__main__":
    unittest.main()
