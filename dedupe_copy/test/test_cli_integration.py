"""
Integration tests that run the CLI in a subprocess to simulate real-world usage.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest


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
        Tests the failing --no-walk scenario by running commands in separate
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
                "--min-delete-size",
                "4",  # c.txt and d.txt are smaller than this
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            run_result.returncode, 0, f"No-walk run failed: {run_result.stderr}"
        )
        output = run_result.stdout

        # Assert that the duplicates are found and processed for deletion
        self.assertIn(
            "[DRY RUN] Would delete",
            output,
            "Dry run deletion message not found in output.",
        )
        self.assertIn(
            "Starting deletion of 1 files.",
            output,
            "Incorrect number of files reported for deletion.",
        )
        self.assertIn(
            "Skipping deletion of files with size 3 bytes",
            output,
            "Size threshold message not found.",
        )

        # Check that original files are still there (because it's a dry run)
        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 5)

    def test_no_walk_delete_with_subprocess(self):
        """
        Tests the failing --no-walk scenario by running commands in separate
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
        run_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dedupe_copy",
                "--no-walk",
                "--delete",
                "-i",
                self.manifest_path,
                "--min-delete-size",
                "4",  # c.txt and d.txt are smaller than this
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            run_result.returncode, 0, f"No-walk run failed: {run_result.stderr}"
        )
        output = run_result.stdout

        self.assertIn(
            "Starting deletion of 1 files.",
            output,
            "Incorrect number of files reported for deletion.",
        )
        self.assertIn(
            "Skipping deletion of files with size 3 bytes",
            output,
            "Size threshold message not found.",
        )

        # Confirm correct deletions occurred
        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 3)
        self.assertIn("a.txt", remaining_files)
        self.assertIn("b.txt", remaining_files)
        self.assertIn("e.txt", remaining_files)
        self.assertNotIn("c.txt", remaining_files)
        self.assertNotIn("d.txt", remaining_files)
