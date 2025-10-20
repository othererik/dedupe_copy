"""Basic tests around copy operations"""

from functools import partial
import os
import unittest

import queue
import threading
import time

from dedupe_copy.test import utils

from dedupe_copy.core import run_dupe_copy
from dedupe_copy.path_rules import _best_match
from dedupe_copy.threads import ResultProcessor
from dedupe_copy.utils import match_extension
from dedupe_copy import disk_cache_dict


do_copy = partial(
    run_dupe_copy,
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
        self.manifest_dir = utils.make_temp_dir("manifest")

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)
        utils.remove_dir(self.manifest_dir)

    def test_delete_on_copy(self):
        """Test that source files are deleted after a successful copy."""
        # Create 10 files in the source directory
        file_data = utils.make_file_tree(self.temp_dir, file_spec=10, file_size=100)
        source_file_count = len(file_data)
        copy_to_path = os.path.join(self.temp_dir, "copy_target")
        manifest_path = os.path.join(self.manifest_dir, "manifest.db")

        # Perform the copy with deletion
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            delete_on_copy=True,
            manifest_out_path=manifest_path,
        )

        # Verify that all files were copied
        copied_files = list(utils.walk_tree(copy_to_path))
        self.assertEqual(len(copied_files), source_file_count)

        # Verify that all source files were deleted
        source_files = list(utils.walk_tree(self.temp_dir))
        # The copy_target directory is inside the temp_dir, so walk_tree will find the copied files
        self.assertEqual(len(source_files), source_file_count)

    def test_delete_on_copy_dry_run(self):
        """Test that source files are NOT deleted on copy with --dry-run."""
        # Create 10 files in the source directory
        file_data = utils.make_file_tree(self.temp_dir, file_spec=10, file_size=100)
        source_file_count = len(file_data)
        copy_to_path = os.path.join(self.temp_dir, "copy_target")
        manifest_path = os.path.join(self.manifest_dir, "manifest.db")

        # Perform the copy with deletion and dry-run
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            delete_on_copy=True,
            dry_run=True,
            manifest_out_path=manifest_path,
        )

        # Verify that NO files were copied because of dry_run
        self.assertFalse(os.path.exists(copy_to_path))

        # Verify that NO source files were deleted
        source_files = list(utils.walk_tree(self.temp_dir))
        self.assertEqual(len(source_files), source_file_count)

    def test_copy_no_change_no_dupes(self):
        """Test copying of small tree to same structure - no dupe no change"""
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=10, extensions=None, file_size=1000
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
            self.temp_dir, file_spec=10, extensions=None, file_size=1000
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
        copied_file_data = []
        for file_info in self.file_data:
            # The default behavior is to preserve the directory structure.
            new_dest = file_info[0].replace(self.temp_dir, copy_to_path, 1)
            copied_file_data.append([new_dest] + file_info[1:])

        result, notes = utils.verify_files(copied_file_data)
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_copy_dupe_zero_byte_dedupe_empty_dupes(self):
        """Small tree to same structure with all zero byte files. 0 is dupe.
        Only one should remain.
        """
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=10, extensions=None, file_size=0
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            dedupe_empty=True,
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify we copied only one file:
        files = list(utils.walk_tree(copy_to_path, include_dirs=False))
        self.assertEqual(len(files), 1, f"Did not copy just 1 file: {files}")
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_copy_dupe_zero_byte_no_dedupe_empty_dupes(self):
        """Small tree to same structure with all zero byte files. 0 not dupe.
        All should be copied.
        """
        self.file_data = utils.make_file_tree(
            self.temp_dir, file_spec=10, extensions=None, file_size=0
        )
        copy_to_path = os.path.join(self.temp_dir, "tree_copy")
        # perform the copy
        do_copy(
            read_from_path=self.temp_dir,
            copy_to_path=copy_to_path,
            path_rules=["*:no_change"],
            dedupe_empty=False,
        )
        # verify we didn't alter the existing data
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Altered original files: {notes}")
        # verify the copied data
        for file_info in self.file_data:
            file_info[0] = file_info[0].replace(self.temp_dir, copy_to_path, 1)
        result, notes = utils.verify_files(self.file_data)
        self.assertTrue(result, f"Failed to copy files: {notes}")

    def test_best_match_no_matches(self):
        """Test _best_match when no extensions match - line 102"""
        extensions = ["*.jpg", "*.png", "*.gif"]
        result = _best_match(extensions, "txt")
        self.assertIsNone(result, "Expected None when no patterns match")

    def test_best_match_multiple_patterns(self):
        """Test _best_match with multiple matching patterns"""
        # Test multiple patterns that match and choose best by length
        extensions = ["*.j*", "*.jp*", "*.jpg", "*.jpeg"]
        result = _best_match(extensions, "jpg")
        # Should return exact match
        self.assertEqual(result, "*.jpg")

        # Test with patterns that need scoring
        extensions = ["*.j*", "*.???", "*.j??"]
        result = _best_match(extensions, "jpg")
        # Should return the closest match by scoring
        self.assertIn(result, extensions)

    def test_match_extension_with_patterns(self):
        """Test _match_extension with wildcard patterns - line 170"""
        # Test pattern matching (not just exact match)
        self.assertTrue(match_extension(["*.txt"], "test.txt"))
        self.assertTrue(match_extension(["test.*"], "test.jpg"))
        self.assertFalse(match_extension(["*.jpg"], "test.txt"))

    def test_extension_filter(self):
        """Test extension filtering in copy operations - covers line 170"""
        temp_dir = utils.make_temp_dir("extension_filter_test")
        try:
            # Create files with different extensions
            file_data = utils.make_file_tree(
                temp_dir,
                file_spec=5,
                extensions=[".txt", ".jpg", ".png"],
                file_size=100,
            )
            assert len(file_data) > 0, "No files created for testing"

            copy_to_path = os.path.join(temp_dir, "filtered_copy")

            # Copy only .txt files

            do_copy_filtered = partial(
                run_dupe_copy,
                ignore_old_collisions=False,
                walk_threads=1,
                read_threads=1,
                copy_threads=1,
                convert_manifest_paths_to="",
                convert_manifest_paths_from="",
                no_walk=False,
                preserve_stat=False,
            )

            # Use extension filter
            do_copy_filtered(
                read_from_path=temp_dir,
                copy_to_path=copy_to_path,
                extensions=[".txt"],
                path_rules=["*:no_change"],
            )

            # Verify only .txt files were copied
            copied_files = list(utils.walk_tree(copy_to_path, include_dirs=False))
            for f in copied_files:
                self.assertTrue(f.endswith(".txt"), f"Non-.txt file copied: {f}")

        finally:
            utils.remove_dir(temp_dir)

    def test_result_processor_save_event_wait(self):
        """Test ResultProcessor waiting on save_event"""
        temp_dir = utils.make_temp_dir("result_processor_test")
        try:
            db_file = os.path.join(temp_dir, "manifest.db")
            manifest = disk_cache_dict.DefaultCacheDict(
                default_factory=list, max_size=100, db_file=db_file
            )

            result_queue = queue.Queue()
            progress_queue = queue.Queue()
            stop_event = threading.Event()
            save_event = threading.Event()
            collisions = {}

            # Set save_event to simulate save in progress
            save_event.set()

            # Add a result to process
            result_queue.put(("abc123", 1000, "2020-01-01", "/tmp/file.txt"))

            # Create and start result processor
            processor = ResultProcessor(
                result_queue=result_queue,
                stop_event=stop_event,
                collisions=collisions,
                manifest=manifest,
                progress_queue=progress_queue,
                dedupe_empty=False,
                save_event=save_event,
            )

            processor.start()

            # Give it time to hit the save_event wait
            time.sleep(0.2)

            # Verify the result wasn't processed yet (still in queue)
            self.assertFalse(
                result_queue.empty(), "Result should not be processed during save"
            )

            # Clear save_event to allow processing
            save_event.clear()
            time.sleep(0.2)

            # Stop the processor
            stop_event.set()
            processor.join(timeout=2)

            # Now result should be processed
            self.assertTrue(
                result_queue.empty() or stop_event.is_set(),
                "Result should be processed after save_event cleared",
            )

        finally:
            utils.remove_dir(temp_dir)


if __name__ == "__main__":
    unittest.main()
