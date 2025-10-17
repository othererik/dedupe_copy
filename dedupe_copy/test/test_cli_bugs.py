"""
Tests for specific bugs reported against the CLI or core logic.
"""
import os
import tempfile
import unittest

from dedupe_copy.core import run_dupe_copy
from dedupe_copy.test.utils import make_file_tree


class TestCliBugs(unittest.TestCase):
    """Tests for CLI and core logic bugs."""

    def test_keep_empty_flag_copies_empty_files(self):
        """Verify that --keep-empty actually copies empty files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, "source")
            dest_dir = os.path.join(temp_dir, "dest")
            os.makedirs(source_dir)
            os.makedirs(dest_dir)

            # Create a non-empty file
            with open(os.path.join(source_dir, "file1.txt"), "w") as f:
                f.write("not empty")

            # Create two empty files
            with open(os.path.join(source_dir, "empty1.txt"), "w") as f:
                pass
            with open(os.path.join(source_dir, "empty2.txt"), "w") as f:
                pass

            run_dupe_copy(
                read_from_path=source_dir,
                copy_to_path=dest_dir,
                keep_empty=True,
                hash_algo="md5",
                path_rules=["*:no_change"],  # Disable default path rules for simplicity
            )

            copied_files = sorted(os.listdir(dest_dir))
            expected_files = ["empty1.txt", "empty2.txt", "file1.txt"]
            self.assertListEqual(
                copied_files,
                expected_files,
                "All files, including all empty files, should have been copied.",
            )