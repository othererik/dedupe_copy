"""
Consolidated file for all user scenario, integration, and command-line flag interaction tests.
"""

import os
import shutil
import subprocess
import sys
import tempfile
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


class TestCliIntegration(unittest.TestCase):
    """Test the CLI by running it as a separate process."""

    def setUp(self):
        """Set up a test environment with pre-existing files."""
        self.root = tempfile.mkdtemp()
        self.files_dir = os.path.join(self.root, "files")
        os.makedirs(self.files_dir)
        self.manifest_path = os.path.join(self.root, "manifest.db")

        # Create a common set of files for all tests
        self._create_file("a.txt", "content1")  # 8 bytes, duplicate
        self._create_file("b.txt", "content1")  # 8 bytes, duplicate
        self._create_file("c.txt", "sho")  # 3 bytes, duplicate
        self._create_file("d.txt", "sho")  # 3 bytes, duplicate
        self._create_file("e.txt", "unique")  # 6 bytes, unique

    def tearDown(self):
        shutil.rmtree(self.root)

    def _create_file(self, name, content):
        """Creates a file with specified name and content in the files_dir"""
        path = os.path.join(self.files_dir, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def test_no_walk_delete_dry_run_with_subprocess(self):
        """
        Tests --no-walk scenario by running commands in separate
        subprocesses to ensure manifest file handles are closed and reopened.
        """
        # 1. Generate manifest in a separate process
        gen_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "-p",
                self.files_dir,
                "-m",
                self.manifest_path,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            gen_result.returncode, 0, f"Manifest generation failed: {gen_result.stderr}"
        )
        self.assertTrue(os.path.exists(self.manifest_path))

        # 2. Run with --no-walk and --delete in a separate process
        try:
            run_result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "dedupe_copy",
                    "--no-walk",
                    "--delete",
                    "--dry-run",
                    "-i",
                    self.manifest_path,
                    "-m",
                    self.manifest_path + ".new",
                    "--min-delete-size",
                    "4",  # c.txt and d.txt are smaller than this
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self.fail(f"No-walk run failed with stderr:\n{e.stderr}")
        output = run_result.stdout

        # Assert that the duplicates are found and processed for deletion
        self.assertIn(
            "DRY RUN: Would have started deletion of 1 files.",
            output,
            "Dry run deletion message not found in output.",
        )
        self.assertIn(
            "Skipping deletion of ",
            output,
            "Size threshold message not found.",
        )
        self.assertIn(
            "with size 3 bytes",
            output,
            "Size threshold message not found.",
        )

        # Check that original files are still there (because it's a dry run)
        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 5)

    def test_no_walk_delete_with_subprocess(self):
        """
        Tests  --no-walk scenario by running commands in separate
        subprocesses to ensure manifest file handles are closed and reopened.
        """
        # 1. Generate manifest in a separate process
        gen_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "-p",
                self.files_dir,
                "-m",
                self.manifest_path,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            gen_result.returncode, 0, f"Manifest generation failed: {gen_result.stderr}"
        )
        self.assertTrue(os.path.exists(self.manifest_path))

        # 2. Run with --no-walk and --delete in a separate process
        try:
            run_result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "dedupe_copy",
                    "--no-walk",
                    "--delete",
                    "-i",
                    self.manifest_path,
                    "-m",
                    self.manifest_path + ".new",
                    "--min-delete-size",
                    "4",  # c.txt and d.txt are smaller than this
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self.fail(f"No-walk run failed with stderr:\n{e.stderr}")
        output = run_result.stdout

        self.assertIn(
            "Starting deletion of 1 files.",
            output,
            "Incorrect number of files reported for deletion.",
        )
        self.assertIn(
            "Skipping deletion of ",
            output,
            "Size threshold message not found.",
        )
        self.assertIn(
            "with size 3 bytes",
            output,
            "Size threshold message not found.",
        )

        # Confirm correct deletions occurred
        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 4)
        self.assertIn("a.txt", remaining_files)
        self.assertNotIn("b.txt", remaining_files)
        self.assertIn("c.txt", remaining_files)
        self.assertIn("d.txt", remaining_files)
        self.assertIn("e.txt", remaining_files)

    def test_walk_delete_with_subprocess(self):
        """
        Test deleting while walking
        """
        run_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "--delete",
                "-p",
                self.files_dir,
                "-m",
                self.manifest_path,
                "--min-delete-size",
                "4",  # c.txt and d.txt are smaller than this
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            run_result.returncode, 0, f"Walk and delete run failed: {run_result.stderr}"
        )
        output = run_result.stdout

        self.assertIn(
            "Starting deletion of 1 files.",
            output,
            "Incorrect number of files reported for deletion.",
        )
        self.assertIn(
            "Skipping deletion of ",
            output,
            "Size threshold message not found.",
        )
        self.assertIn(
            "with size 3 bytes",
            output,
            "Size threshold message not found.",
        )

        # Confirm correct deletions occurred
        remaining_files = os.listdir(self.files_dir)
        self.assertIn("c.txt", remaining_files)
        self.assertIn("d.txt", remaining_files)
        self.assertIn("e.txt", remaining_files)
        # one of the dupes is kept
        self.assertEqual(len(remaining_files), 4)

    def test_verify_with_subprocess(self):
        """
        Tests manifest verification via the --verify flag.
        """
        # 1. Generate manifest
        gen_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "-p",
                self.files_dir,
                "-m",
                self.manifest_path,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            gen_result.returncode, 0, f"Manifest generation failed: {gen_result.stderr}"
        )

        # 2. Run verification (success case)
        verify_success_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "--no-walk",
                "--verify",
                "--manifest-read-path",
                self.manifest_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn(
            "Manifest verification successful.",
            verify_success_result.stdout,
            "Success message not found in verification output.",
        )

        # 3. Delete a file to trigger a verification failure
        os.remove(os.path.join(self.files_dir, "a.txt"))

        # 4. Run verification again (failure case)
        verify_fail_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "--no-walk",
                "--verify",
                "--manifest-read-path",
                self.manifest_path,
            ],
            capture_output=True,
            text=True,
            check=False,  # Expect a non-zero exit code
        )
        self.assertIn(
            "Manifest verification failed.",
            verify_fail_result.stdout,
            "Failure message not found in verification output.",
        )
        self.assertIn(
            "VERIFY FAILED: File not found",
            verify_fail_result.stdout,
            "File not found error not in verification output.",
        )


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
