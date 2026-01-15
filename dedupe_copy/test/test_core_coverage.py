"""Targeted tests to improve coverage for dedupe_copy.core"""

import unittest
import queue
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch, ANY

from dedupe_copy.core import (
    _walk_fs,
    generate_report,
    delete_files,
    verify_manifest_fs,
    run_dupe_copy,
)
from dedupe_copy.config import WalkConfig, DeleteJob


class TestCoreCoverage(unittest.TestCase):
    """Targeted tests to improve coverage for dedupe_copy.core"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_walk_fs_default_queue(self):
        """Test _walk_fs instantiates walk_queue if None provided (Line 73)"""
        read_paths = []
        walk_config = WalkConfig()
        work_queue = queue.Queue()
        # Mock threads to avoid actual execution
        with patch("dedupe_copy.core.WalkThread") as mock_thread:
            _walk_fs(
                read_paths,
                walk_config,
                work_queue=work_queue,
                walk_queue=None,  # This should trigger the line
                already_processed=set(),
                walk_threads=1,
            )
            # Just ensuring it didn't crash and threads were started
            self.assertTrue(mock_thread.called)

    def test_generate_report_io_error(self):
        """Test generate_report handles IOError (Line 169-170)"""
        # Create a directory where file should be to cause IsADirectoryError or similar
        report_path = os.path.join(self.temp_dir, "dir_as_file")
        os.makedirs(report_path)

        with patch("dedupe_copy.core.logger") as mock_logger:
            generate_report(report_path, {}, [], "md5")
            mock_logger.error.assert_called_with(
                "Could not write report to %s: %s", report_path, ANY
            )

    def test_delete_files_empty_file_list(self):
        """Test delete_files with empty file list (Line 619)"""
        # Manually constructing duplicates dict with empty list
        duplicates = {"hash1": []}
        delete_job = DeleteJob(delete_threads=1)

        deleted = delete_files(duplicates, None, delete_job=delete_job)
        self.assertEqual(deleted, [])

    def test_delete_files_skip_empty_no_dedupe(self):
        """Test delete_files skipping empty file when dedupe_empty=False (Lines 633-645)"""
        # Setup: file size 0, dedupe_empty=False in job
        duplicates = {
            "hash1": [
                ("/path/keep", 0, 100),
                ("/path/delete", 0, 100),  # This one would be deleted
            ]
        }
        delete_job = DeleteJob(delete_threads=1, dedupe_empty=False)
        progress_queue = queue.PriorityQueue()

        delete_files(duplicates, progress_queue, delete_job=delete_job)

        # Check for the specific log message in queue
        found_msg = False
        while not progress_queue.empty():
            item = progress_queue.get()
            if "Skipping deletion of empty file" in str(item):
                found_msg = True
                break
        self.assertTrue(found_msg, "Should log message about skipping empty file")

    def test_verify_manifest_fs_ui_and_errors(self):
        """Test verify_manifest_fs with UI objects and OSError (Lines 744, 769-771, 773)"""
        manifest = MockManifest()
        # Add a mock item: path, size, mtime
        start_path = os.path.join(self.temp_dir, "missing.txt")
        manifest.items_dict = {"hash1": [[start_path, 100, 12345]]}

        mock_ui = MagicMock()
        mock_ui.add_task.return_value = 1  # Ensure a task_id is returned

        # 1. Test missing file (Line 754) is already covered, but let's Ensure UI is called
        result = verify_manifest_fs(manifest, ui=mock_ui)
        self.assertFalse(result)
        mock_ui.add_task.assert_called()  # Line 744
        mock_ui.update_task.assert_called()  # Line 773

        # 2. Test OSError (Line 769-771)
        # We need os.path.exists to raise OSError or getsize to raise it
        # Let's mock os.path.exists to pass, then getsize to fail
        manifest.items_dict = {
            "hash2": [[os.path.join(self.temp_dir, "error.txt"), 100, 12345]]
        }
        with (
            patch("dedupe_copy.core.os.path.exists", return_value=True),
            patch(
                "dedupe_copy.core.os.path.getsize", side_effect=OSError("Access denied")
            ),
            patch("dedupe_copy.core.logger") as mock_logger,
        ):

            result = verify_manifest_fs(manifest, ui=None)
            self.assertFalse(result)
            mock_logger.error.assert_called()
            # Verify the specific error message for OSError
            call_args = mock_logger.error.call_args_list
            self.assertTrue(any("Could not access" in str(c) for c in call_args))

    def test_run_dupe_copy_verify_cleanup_error(self):
        """Test run_dupe_copy verify mode cleanup error (Lines 950-962)"""
        # trigger verify_manifest=True
        with (
            patch("dedupe_copy.core.Manifest"),
            patch("dedupe_copy.core.verify_manifest_fs"),
            patch(
                "dedupe_copy.core.shutil.rmtree", side_effect=OSError("Cleanup fail")
            ),
            patch("dedupe_copy.core.logger") as mock_logger,
        ):

            run_dupe_copy(verify_manifest=True, use_ui=False)

            mock_logger.warning.assert_called_with(
                "Failed to cleanup the temp_directory: %s with err: %s", ANY, ANY
            )

    def test_run_dupe_copy_no_copy_list(self):
        """Test run_dupe_copy with no_copy list (Lines 965-966)"""
        # This exercises the loop setting compare[item] = None
        mock_compare = MagicMock()
        with patch("dedupe_copy.core.Manifest", return_value=mock_compare):
            run_dupe_copy(no_copy=["hash1"], verify_manifest=False, use_ui=False)
            mock_compare.__setitem__.assert_called_with("hash1", None)

    def test_run_dupe_copy_no_walk_error(self):
        """Test run_dupe_copy no_walk without manifest raises ValueError (Line 970)"""
        with self.assertRaisesRegex(ValueError, "If --no-walk is specified"):
            run_dupe_copy(no_walk=True, manifests_in_paths=None)

    def test_run_dupe_copy_delete_and_copy_error(self):
        """Test run_dupe_copy conflict delete and copy (Line 1068)"""
        with (
            patch("dedupe_copy.core.logger") as mock_logger,
            patch("dedupe_copy.core.find_duplicates", return_value=({}, MagicMock())),
            patch("dedupe_copy.core._extension_report"),
        ):

            run_dupe_copy(
                read_from_path=self.temp_dir,
                delete_duplicates=True,
                copy_to_path="/tmp/dest",
            )
            mock_logger.error.assert_called_with(
                "Cannot use --delete and --copy-path at the same time."
            )

    def test_run_dupe_copy_cleanup_error_finally(self):
        """Test run_dupe_copy final cleanup error (Line 1201-1202)"""
        # We need to run enough to reach finally block
        # use verify_manifest=True is simplest, but we covered that above.
        # Let's use a normal run with mocked rmtree
        with (
            patch("dedupe_copy.core.find_duplicates", return_value=({}, MagicMock())),
            patch("dedupe_copy.core._extension_report"),
            patch(
                "dedupe_copy.core.shutil.rmtree",
                side_effect=OSError("Final cleanup fail"),
            ),
            patch("dedupe_copy.core.logger") as mock_logger,
        ):

            run_dupe_copy(read_from_path=self.temp_dir, use_ui=False)

            mock_logger.warning.assert_called_with(
                "Failed to cleanup the temp_directory: %s with err: %s", ANY, ANY
            )


class MockManifest:
    """Helper to mock Manifest object behavior for iteration"""

    def __init__(self):
        """Initialize mock manifest"""
        self.items_dict = {}

    def items(self):
        """Return items from dict"""
        return self.items_dict.items()

    def __len__(self):
        """Return length of items dict"""
        return len(self.items_dict)
