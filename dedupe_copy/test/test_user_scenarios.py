"""
Tests for complex, multi-flag user scenarios to ensure the tool behaves as
expected and that the manifest and filesystem are left in a consistent state.
"""

import os
import shutil
import unittest
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli
from dedupe_copy.test.utils import make_file_tree, walk_tree


class TestUserScenarios(unittest.TestCase):
    """Test suite for complex user scenarios."""

    def setUp(self):
        """Set up a temporary directory for each test."""
        self.temp_dir = "temp_test_user_scenarios"
        os.makedirs(self.temp_dir, exist_ok=True)
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        self.compare_dir = os.path.join(self.temp_dir, "compare")
        os.makedirs(self.source_dir, exist_ok=True)
        os.makedirs(self.dest_dir, exist_ok=True)
        os.makedirs(self.compare_dir, exist_ok=True)

    def tearDown(self):
        """Remove the temporary directory after each test."""
        shutil.rmtree(self.temp_dir)

    def _run_cli(self, args):
        """Helper to run the CLI with a given set of arguments."""
        with patch("sys.argv", ["dedupecopy"] + args):
            run_cli()

    def _get_filenames_in_dir(self, directory):
        """Returns a sorted list of all filenames in a directory tree."""
        return sorted(list(walk_tree(directory)))

    def test_compare_and_delete(self):
        """Verify that --delete removes files present in a --compare manifest."""
        # 1. Setup: Create files in source and compare directories
        make_file_tree(
            self.source_dir,
            {
                "dup1.txt": "content1",
                "dup2.txt": "content2",
                "unique.txt": "unique_content",
            },
        )
        make_file_tree(
            self.compare_dir, {"comp1.txt": "content1", "comp2.txt": "content2"}
        )

        # 2. Generate the manifest for the compare directory
        compare_manifest_path = os.path.join(self.temp_dir, "compare.db")
        self._run_cli(["-p", self.compare_dir, "-m", compare_manifest_path])

        # 3. Run the delete operation on the source directory
        output_manifest_path = os.path.join(self.temp_dir, "output.db")
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "--delete",
                "--compare",
                compare_manifest_path,
                "-m",
                output_manifest_path,
            ]
        )

        # 4. Verify the correct files were deleted from the source directory
        remaining_files = self._get_filenames_in_dir(self.source_dir)
        self.assertEqual(
            len(remaining_files),
            1,
            "Only the unique file should remain in the source directory.",
        )
        self.assertIn(
            os.path.join(self.source_dir, "unique.txt"),
            remaining_files,
            "The unique file was incorrectly deleted.",
        )

    def test_delete_on_copy_with_same_manifest_is_disallowed(self):
        """Verify that using the same manifest for input and output with a destructive
        operation raises an error."""
        # 1. Setup: Create source files and generate an initial manifest
        make_file_tree(
            self.source_dir,
            {"file1.txt": "contentA", "file2.txt": "contentB"},
        )
        manifest_path = os.path.join(self.temp_dir, "manifest.db")
        self._run_cli(["-p", self.source_dir, "-m", manifest_path])

        # 2. Assert that the CLI exits with an error when the same manifest is used
        # for a destructive operation.
        with self.assertRaises(SystemExit) as cm:
            self._run_cli(
                [
                    "--no-walk",
                    "-i",
                    manifest_path,
                    "-c",
                    self.dest_dir,
                    "--delete-on-copy",
                    "-m",
                    manifest_path,
                ]
            )
        self.assertEqual(cm.exception.code, 2)
