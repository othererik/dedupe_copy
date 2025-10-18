"""Tests for some basics of the logic in the core module."""

import unittest
from unittest.mock import MagicMock

from dedupe_copy.core import delete_files, DeleteJob


class TestCoreLogic(unittest.TestCase):
    """Test core logic"""

    def test_delete_files_dry_run_logging(self):
        """
        Verify that when --dry-run is passed to delete_files, the log reflects that
        no files will be deleted.
        """
        # Set up duplicates data
        duplicates = {
            "hash1": [("file1.txt", 100, 12345), ("file2.txt", 100, 12345)],
        }

        # Mock the progress_queue
        mock_progress_queue = MagicMock()

        # Set up DeleteJob with dry_run=True
        delete_job = DeleteJob(
            delete_threads=1,
            dry_run=True,
            min_delete_size_bytes=0,
        )

        # Call the function
        with self.assertLogs("dedupe_copy.core", level="INFO") as cm:
            delete_files(duplicates, mock_progress_queue, delete_job=delete_job)
            # Check for the correct log message
            self.assertIn("DRY RUN: Would have deleted 1 files.", cm.output[0])
