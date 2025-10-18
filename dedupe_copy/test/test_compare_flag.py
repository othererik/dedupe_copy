"""
Tests for the --compare flag and its interactions.
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli
from dedupe_copy.test.utils import make_file_tree


class TestCompareFlag(unittest.TestCase):
    """Test suite for --compare flag functionality."""

    def setUp(self):
        """Set up a temporary directory with a standard file structure."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_compare_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        self.compare_dir1 = os.path.join(self.temp_dir, "compare1")
        self.compare_dir2 = os.path.join(self.temp_dir, "compare2")

        # Create a set of files in the source directory
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "content1",
                "file2.txt": "content2",
                "file3.txt": "content3",
                "unique_file.txt": "unique_content",
            },
        )

        # Create files for the compare manifests
        make_file_tree(self.compare_dir1, {"file1.txt": "content1"})
        make_file_tree(self.compare_dir2, {"file2.txt": "content2"})

        # Create the destination directory
        os.makedirs(self.dest_dir)

    def tearDown(self):
        """Remove the temporary directory."""
        shutil.rmtree(self.temp_dir)

    def _run_cli(self, args):
        """Helper to run the CLI with a given set of arguments."""
        original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        try:
            with patch("sys.argv", ["dedupecopy"] + args):
                run_cli()
        finally:
            os.chdir(original_cwd)

    def _get_filenames_in_dir(self, directory):
        """Returns a sorted list of all filenames in a directory tree."""
        filenames = []
        for _, _, files in os.walk(directory):
            for file in files:
                filenames.append(file)
        return sorted(filenames)

    def test_copy_no_compare(self):
        """Test copy behavior without the --compare flag."""
        # Run dedupecopy to copy files from source to dest
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-m",
                "manifest.db",
                "--verbose",
            ]
        )

        # Verify that all files from source are in dest
        source_files = self._get_filenames_in_dir(self.source_dir)
        dest_files = self._get_filenames_in_dir(self.dest_dir)
        self.assertEqual(source_files, dest_files)

    def test_copy_with_single_compare(self):
        """Test copy with a single --compare manifest."""
        # 1. Create a manifest for the first compare directory
        self._run_cli(["-p", self.compare_dir1, "-m", "compare1.db"])

        # 2. Run the copy operation with --compare
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--compare",
                "compare1.db",
                "-m",
                "manifest.db",
                "--verbose",
            ]
        )

        # 3. Verify the correct files were copied
        dest_files = self._get_filenames_in_dir(self.dest_dir)
        self.assertEqual(dest_files, ["file2.txt", "file3.txt", "unique_file.txt"])

    def test_copy_with_multiple_compare(self):
        """Test copy with multiple --compare manifests."""
        # 1. Create manifests for both compare directories
        self._run_cli(["-p", self.compare_dir1, "-m", "compare1.db"])
        self._run_cli(["-p", self.compare_dir2, "-m", "compare2.db"])

        # 2. Run the copy operation with multiple --compare flags
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--compare",
                "compare1.db",
                "--compare",
                "compare2.db",
                "-m",
                "manifest.db",
                "--verbose",
            ]
        )

        # 3. Verify the correct files were copied
        dest_files = self._get_filenames_in_dir(self.dest_dir)
        self.assertEqual(dest_files, ["file3.txt", "unique_file.txt"])

    def test_compare_and_delete_on_copy(self):
        """Test interaction of --compare and --delete-on-copy."""
        # 1. Create a manifest for the compare directory
        self._run_cli(["-p", self.compare_dir1, "-m", "compare1.db"])

        # 2. Run the copy with --compare and --delete-on-copy
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--compare",
                "compare1.db",
                "--delete-on-copy",
                "-m",
                "manifest.db",
                "--verbose",
            ]
        )

        # 3. Verify files not in compare manifest were copied
        dest_files = self._get_filenames_in_dir(self.dest_dir)
        self.assertEqual(dest_files, ["file2.txt", "file3.txt", "unique_file.txt"])

        # 4. Verify all original source files were deleted
        source_files = self._get_filenames_in_dir(self.source_dir)
        self.assertEqual(source_files, [])

    def test_compare_path_same_as_output_path_fails(self):
        """Test that using the same path for --compare and -m fails."""
        manifest_path = "the_same_manifest.db"

        # The CLI should exit with an error, which raises SystemExit
        with self.assertRaises(SystemExit):
            self._run_cli(
                [
                    "--no-walk",  # a walk option is required
                    "--compare",
                    manifest_path,
                    "-m",
                    manifest_path,
                ]
            )
