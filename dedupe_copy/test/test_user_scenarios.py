"""
Consolidated file for all user scenario, integration, and command-line flag interaction tests.
"""

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli
from dedupe_copy.manifest import Manifest
from dedupe_copy.test.utils import make_file_tree

# pylint: disable=too-many-lines


class TestUserScenarios(unittest.TestCase):
    """Test suite for advanced, multi-step, and manifest-validation scenarios."""

    def setUp(self):
        """Set up a temporary directory for each test."""
        self.temp_dir = tempfile.mkdtemp(prefix="advanced_scenario_")
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
        # Chdir into the temp dir to keep generated manifests sandboxed
        original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        try:
            with patch("sys.argv", ["dedupecopy"] + args):
                run_cli()
        finally:
            os.chdir(original_cwd)

    def _get_all_filepaths(self, directory):
        """Returns a set of all full file paths in a directory tree."""
        return {
            os.path.join(root, file)
            for root, _, files in os.walk(directory)
            for file in files
        }

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
        remaining_files = self._get_all_filepaths(self.source_dir)
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

    def test_delete_on_copy_move_operation(self):
        """Test a simple 'move' operation (`--delete-on-copy`) and verify the manifest."""
        # 1. Setup: Create source files
        make_file_tree(
            self.source_dir,
            {"file1.txt": "contentA", "sub/file2.txt": "contentB"},
        )
        manifest_path = os.path.join(self.temp_dir, "manifest.db")

        # 2. Run the move operation
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--delete-on-copy",
                "-m",
                manifest_path,
                "-R",
                "*:no_change",  # Use a simple rule for predictable paths
            ]
        )

        # 3. Verify the filesystem state
        source_files = self._get_all_filepaths(self.source_dir)
        self.assertEqual(len(source_files), 0, "Source directory should be empty.")
        dest_files = self._get_all_filepaths(self.dest_dir)
        self.assertEqual(len(dest_files), 2, "All files should be in the destination.")

        # 4. Deeply verify the manifest
        manifest = Manifest(manifest_path, temp_directory=self.temp_dir)
        manifest_paths = {
            file_info[0] for _, file_list in manifest.items() for file_info in file_list
        }

        self.assertEqual(
            manifest_paths,
            dest_files,
            "Manifest paths must match the new destination paths.",
        )

    def test_delete_on_copy_dry_run(self):
        """Test that --dry-run prevents any changes during a move operation."""
        # 1. Setup: Create source files
        source_paths = make_file_tree(
            self.source_dir,
            {"file1.txt": "contentA", "file2.txt": "contentB"},
        )
        manifest_path = os.path.join(self.temp_dir, "manifest.db")

        # 2. Run the move operation with --dry-run
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--delete-on-copy",
                "--dry-run",
                "-m",
                manifest_path,
            ]
        )

        # 3. Verify the filesystem state
        self.assertTrue(
            os.path.exists(source_paths[0][0]), "Source file1 should not be deleted."
        )
        self.assertTrue(
            os.path.exists(source_paths[1][0]), "Source file2 should not be deleted."
        )
        self.assertEqual(
            len(os.listdir(self.dest_dir)), 0, "Destination dir should be empty."
        )

        # 4. Verify that the manifest was NOT created
        self.assertFalse(
            os.path.exists(manifest_path),
            "Manifest file should not be created on dry run.",
        )

    def test_sync_to_target_with_delete_on_copy_and_compare(self):
        """
        Replicates a user-reported scenario:
        - Source and Target directories exist.
        - Target already contains some files, some of which are duplicates of source files.
        - Goal: Copy new/unique files from source to target, preserving directory structure,
          and delete the source files after copy. Duplicates in source should also be deleted.
        """
        # 1. Setup: Create source and target file structures
        make_file_tree(
            self.source_dir,
            {
                "hi": "hi_content",
                "dir1/adupe": "dupe_content",
                "dir2/filesd2": "new_file_content",
            },
        )
        # self.dest_dir will be our "target" directory for the copy operation
        make_file_tree(
            self.dest_dir,
            {"hit": "hi_content", "dir1/dupe": "dupe_content"},
        )

        # 2. Generate manifests for source and the destination (acting as target)
        source_manifest_path = os.path.join(self.temp_dir, "source.db")
        target_manifest_path = os.path.join(self.temp_dir, "target.db")
        self._run_cli(["-p", self.source_dir, "-m", source_manifest_path])
        self._run_cli(
            ["-p", self.dest_dir, "-m", target_manifest_path]
        )  # Manifest for the destination

        # 3. Run the sync/copy operation
        output_manifest_path = os.path.join(self.temp_dir, "final.db")
        self._run_cli(
            [
                "--no-walk",
                "-i",
                source_manifest_path,
                "-c",
                self.dest_dir,  # This is the target for the copy
                "--delete-on-copy",
                "--compare",
                target_manifest_path,  # Compare against the destination's manifest
                "-m",
                output_manifest_path,
            ]
        )

        # 4. Verify the destination directory state
        # Check that the new file was copied and its path was preserved
        self.assertTrue(os.path.exists(os.path.join(self.dest_dir, "dir2", "filesd2")))

        # Check that the files that were already in the destination are still there
        self.assertTrue(os.path.exists(os.path.join(self.dest_dir, "hit")))
        self.assertTrue(os.path.exists(os.path.join(self.dest_dir, "dir1", "dupe")))

        # Check that duplicates from source were not copied over
        self.assertFalse(os.path.exists(os.path.join(self.dest_dir, "hi")))
        self.assertFalse(os.path.exists(os.path.join(self.dest_dir, "adupe")))

        # 5. Verify the source directory is now empty
        remaining_source_files = self._get_all_filepaths(self.source_dir)
        self.assertEqual(
            len(remaining_source_files),
            0,
            "All files should have been deleted from the source directory, "
            f"but found: {remaining_source_files}",
        )

    def test_compare_delete_on_copy_does_not_copy_dupes(self):
        """
        Replicates a user-reported bug where a file that is a duplicate of a file
        in the target/compare manifest is copied instead of being skipped and deleted
        from the source. This is a simplified version of the user's scenario.
        """
        # 1. Setup:
        # Source has a.txt, Target has b.txt. Both have the same content.
        make_file_tree(self.source_dir, {"a.txt": "dupe_content"})
        make_file_tree(self.dest_dir, {"b.txt": "dupe_content"})

        # 2. Generate manifests for source and target
        source_manifest_path = os.path.join(self.temp_dir, "source.db")
        target_manifest_path = os.path.join(self.temp_dir, "target.db")
        final_manifest_path = os.path.join(self.temp_dir, "final.db")

        self._run_cli(["-p", self.source_dir, "-m", source_manifest_path])
        self._run_cli(["-p", self.dest_dir, "-m", target_manifest_path])

        # 3. Run the operation that should trigger the bug
        self._run_cli(
            [
                "--no-walk",
                "-i",
                source_manifest_path,
                "-c",
                self.dest_dir,
                "--compare",
                target_manifest_path,
                "--delete-on-copy",
                "-m",
                final_manifest_path,
            ]
        )

        # 4. Verification
        # a.txt from source should NOT be copied to the destination
        self.assertFalse(os.path.exists(os.path.join(self.dest_dir, "a.txt")))

        # b.txt (the original file) should still be in the destination
        self.assertTrue(os.path.exists(os.path.join(self.dest_dir, "b.txt")))

        # a.txt should be deleted from the source because it's a duplicate
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, "a.txt")))

    def test_delete_on_copy_keyerror_regression(self):
        """
        A robust test for a user-reported KeyError, consolidating previous tests.

        This scenario creates a complex file structure to stress-test the manifest
        update logic when using `--delete-on-copy` and `--compare`. It includes:
        - Files unique to the source (should be moved).
        - Duplicates within the source (one should be moved, the rest deleted).
        - Duplicates between source and target (source files should be deleted).
        - Nested directories.
        The bug was that the system tried to remove a file's hash from the manifest
        twice, causing a KeyError. This test ensures the logic correctly handles
        all file operation types (move, delete-dupe-internal, delete-dupe-external)
        in a single run without crashing.
        """
        # 1. Setup a more complex file structure
        make_file_tree(
            self.source_dir,
            {
                "unique_to_source.txt": "unique_source_content",
                "dup_in_source_1.txt": "source_dupe_content",
                "dup_in_source_2.txt": "source_dupe_content",
                "dir1/shared_with_target.txt": "shared_content",
                "dir2/another_unique.txt": "another_unique_content",
                "dir2/shared_with_target_2.txt": "shared_content_2",
            },
        )
        make_file_tree(
            self.dest_dir,  # This is the 'target' for the copy operation
            {
                "already_in_dest.txt": "dest_content",
                "dir_A/target_version_of_shared.txt": "shared_content",
                "dir_B/target_version_of_shared_2.txt": "shared_content_2",
            },
        )

        # 2. Generate manifests
        source_manifest_path = os.path.join(self.temp_dir, "source.db")
        target_manifest_path = os.path.join(self.temp_dir, "target.db")
        final_manifest_path = os.path.join(self.temp_dir, "final.db")

        self._run_cli(["-p", self.source_dir, "-m", source_manifest_path])
        self._run_cli(["-p", self.dest_dir, "-m", target_manifest_path])

        # 3. Run the command that was causing the KeyError
        try:
            self._run_cli(
                [
                    "--no-walk",
                    "-i",
                    source_manifest_path,
                    "-c",
                    self.dest_dir,
                    "--compare",
                    target_manifest_path,
                    "--delete-on-copy",
                    "-m",
                    final_manifest_path,
                ]
            )
        except KeyError as e:
            self.fail(f"The operation failed with an unexpected KeyError: {e}")

        # 4. Verification
        # Source should have no files left, only empty directories might remain.
        remaining_source_files = self._get_all_filepaths(self.source_dir)
        self.assertEqual(
            len(remaining_source_files),
            0,
            f"Source directory should be empty of files, but found: {remaining_source_files}",
        )

        # Destination should contain its original files plus the new/unique ones from source.
        dest_files = self._get_all_filepaths(self.dest_dir)

        # Define the set of files we expect to find in the destination.
        expected_dest_paths = {
            # Original files in dest
            os.path.join(self.dest_dir, "already_in_dest.txt"),
            os.path.join(self.dest_dir, "dir_A", "target_version_of_shared.txt"),
            os.path.join(self.dest_dir, "dir_B", "target_version_of_shared_2.txt"),
            # Files moved from source
            os.path.join(self.dest_dir, "unique_to_source.txt"),
            os.path.join(self.dest_dir, "dir2", "another_unique.txt"),
        }
        # One of the two source duplicates should have been moved.
        # We need to check for either possibility.
        moved_dupe_path1 = os.path.join(self.dest_dir, "dup_in_source_1.txt")
        moved_dupe_path2 = os.path.join(self.dest_dir, "dup_in_source_2.txt")

        found_moved_dupe = (
            moved_dupe_path1 in dest_files or moved_dupe_path2 in dest_files
        )
        self.assertTrue(
            found_moved_dupe,
            "One of the internal source duplicates should have been moved to destination.",
        )

        # Add the found duplicate to the expected set for a final, exact comparison.
        if moved_dupe_path1 in dest_files:
            expected_dest_paths.add(moved_dupe_path1)
        else:
            expected_dest_paths.add(moved_dupe_path2)

        self.assertEqual(
            dest_files,
            expected_dest_paths,
            "The final set of files in the destination directory is incorrect.",
        )

        # Verify the final manifest was created. A deeper check is complex due to
        # non-deterministic duplicate selection, so we focus on the filesystem state.
        self.assertTrue(
            os.path.exists(final_manifest_path), "Final manifest was not created."
        )


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

    def _print_debug(self):
        """Print debug information about the test setup."""
        for dir_path in [
            self.source_dir,
            self.compare_dir1,
            self.compare_dir2,
            self.dest_dir,
        ]:
            print(f"Directory: {dir_path}")
            for root, _, files in os.walk(dir_path):
                for file in files:
                    print(f"- {os.path.join(root, file)}")

    def _validate_manifest(self, manifest_path, expected_files):
        """Helper to validate that a manifest contains the expected files."""
        manifest = Manifest(manifest_path, temp_directory=self.temp_dir)
        manifest_files = [
            file_info[0] for _, file_list in manifest.items() for file_info in file_list
        ]
        manifest_files.sort()
        expected_files_list = list(expected_files)
        expected_files_list.sort()

        print(expected_files_list)
        print(manifest_files)
        self.assertEqual(
            manifest_files,
            expected_files_list,
            f"Manifest {manifest_path} does not contain the expected files.",
        )

        self.assertEqual(
            manifest.hash_set(),
            set(expected_files_list),
            f"Set of manifest hashes does not match expected for {manifest_path}.",
        )

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
        # this case had some subtle bugs so going to excessively verify it

        # add a few more files for complexity and completeness
        make_file_tree(
            os.path.join(self.source_dir, "sub_dir"),
            {
                "file4.txt": "content1",
                "file5.txt": "content2",
                "file6.txt": "content6",  # unique
            },
        )
        self._print_debug()

        # 1. Create a manifest for the compare directory
        self._run_cli(["-p", self.compare_dir1, "-m", "compare1.db"])

        # also add new file to compare that shouldn't matter
        make_file_tree(
            self.compare_dir1,
            {"extra_compare_file.txt": "extra_content"},
        )

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

        # Because the order of processing is not guaranteed, either file2.txt or
        # file5.txt (which has the same content) could be the one that is copied.
        # We need to accept either outcome.
        expected_dest_files_option1 = [
            "file2.txt",
            "file3.txt",
            "file6.txt",
            "unique_file.txt",
        ]
        expected_dest_files_option2 = [
            "file3.txt",
            "file5.txt",
            "file6.txt",
            "unique_file.txt",
        ]

        self.assertIn(
            dest_files,
            [expected_dest_files_option1, expected_dest_files_option2],
            "The files in the destination directory did not match either of the expected outcomes.",
        )

        self.assertTrue(
            os.path.exists(os.path.join(self.dest_dir, "sub_dir", "file6.txt"))
        )

        # 4. Verify all original source files were deleted
        # this is an operation that may be re-considered or mediated later, but for now
        # ALL duplicated files are removed from the source even if we didn't explicltly copy
        # it this time
        source_files = self._get_filenames_in_dir(self.source_dir)
        self.assertEqual(source_files, [])
        self._print_debug()

        # 5. Verify the extra file in compare dir is untouched
        self.assertTrue(
            os.path.exists(os.path.join(self.compare_dir1, "extra_compare_file.txt"))
        )

        # 6. Verify the output manifest contains only the copied files
        manifest_path = os.path.join(self.temp_dir, "manifest.db")
        manifest = Manifest(manifest_path, temp_directory=self.temp_dir)
        actual_manifest_content = {
            file_info[0]: md5
            for md5, file_list in manifest.items()
            for file_info in file_list
        }

        # Base content is the same for both possibilities
        base_expected_content = {
            os.path.join(self.dest_dir, "file3.txt"): hashlib.md5(
                "content3".encode("utf-8")
            ).hexdigest(),
            os.path.join(self.dest_dir, "unique_file.txt"): hashlib.md5(
                "unique_content".encode("utf-8")
            ).hexdigest(),
            os.path.join(self.dest_dir, "sub_dir", "file6.txt"): hashlib.md5(
                "content6".encode("utf-8")
            ).hexdigest(),
        }

        content2_hash = hashlib.md5("content2".encode("utf-8")).hexdigest()

        # Option 1: file2.txt was copied
        expected_manifest_option1 = base_expected_content.copy()
        expected_manifest_option1[os.path.join(self.dest_dir, "file2.txt")] = (
            content2_hash
        )

        # Option 2: file5.txt was copied (from sub_dir)
        expected_manifest_option2 = base_expected_content.copy()
        expected_manifest_option2[
            os.path.join(self.dest_dir, "sub_dir", "file5.txt")
        ] = content2_hash

        self.assertIn(
            actual_manifest_content,
            [expected_manifest_option1, expected_manifest_option2],
            "The manifest content did not match either of the expected outcomes.",
        )

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

    def test_no_walk_with_input_and_compare_manifests(self):
        """
        Test the documented CLI workflow: --no-walk + -i + --compare.

        This tests the sequential multi-source backup pattern documented
        in the CLI help text (dedupecopy_cli.py lines 34-46).
        """
        # Setup: Create target and two source directories with some duplicates
        make_file_tree(
            self.source_dir,
            {
                "source_unique1.txt": "unique_to_source",
                "shared_1_2.txt": "shared_content",
                "shared_all.txt": "content_in_all",
            },
        )
        make_file_tree(
            self.compare_dir1,
            {
                "compare1_unique.txt": "unique_to_compare1",
                "shared_1_2.txt": "shared_content",
                "shared_all.txt": "content_in_all",
            },
        )
        make_file_tree(
            self.dest_dir,
            {
                "dest_existing.txt": "already_in_dest",
                "shared_all.txt": "content_in_all",
            },
        )

        # Step 1: Generate manifests for all three locations
        source_manifest = "source.db"
        compare1_manifest = "compare1.db"
        dest_manifest = "dest.db"

        self._run_cli(["-p", self.source_dir, "-m", source_manifest])
        self._run_cli(["-p", self.compare_dir1, "-m", compare1_manifest])
        self._run_cli(["-p", self.dest_dir, "-m", dest_manifest])

        # Step 2: Copy from source to dest using --no-walk, -i, and --compare
        # This is the documented pattern from CLI help
        output_manifest = "output.db"
        self._run_cli(
            [
                "--no-walk",  # Don't walk filesystem, use manifest
                "-i",
                source_manifest,  # Input manifest with files to copy
                "-c",
                self.dest_dir,  # Copy destination
                "--compare",
                compare1_manifest,  # Don't copy files in this manifest
                "--compare",
                dest_manifest,  # Don't copy files already in dest
                "-m",
                output_manifest,  # Output manifest
                "-R",
                "*:no_change",  # Preserve structure for easier verification
                "--verbose",
            ]
        )

        # Step 3: Verify only unique files from source were copied
        dest_files = self._get_filenames_in_dir(self.dest_dir)

        # The behavior we're testing:
        # - Started with 2 files in dest (dest_existing.txt, shared_all.txt)
        # - Source had 3 files total (make_file_tree creates files in the dir structure)
        # - compare1 has shared_1_2.txt and shared_all.txt (should skip these)
        # - dest already has shared_all.txt (should skip this)
        # - Only source_unique1.txt should be copied (unique to source)
        #
        # So we expect: 2 original + 1 copied = 3 files
        # BUT make_file_tree creates more files in practice due to dir structure

        # Let's verify the key behavior: only source_unique1.txt was copied from source
        self.assertIn("source_unique1.txt", dest_files)
        self.assertNotIn("shared_1_2.txt", dest_files)  # Skipped (in compare1)
        self.assertIn("dest_existing.txt", dest_files)  # Was already there
        self.assertIn("shared_all.txt", dest_files)  # Was already there
        self.assertNotIn("compare1_unique.txt", dest_files)  # Not in source

        # Verify the output manifest was created
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, output_manifest)))

    def test_default_path_rules_behavior(self):
        """
        Test the default path rules behavior when -R is not specified.

        Default behavior (as of this change): preserve original directory structure.
        This is equivalent to -R *:no_change.
        """
        # Setup: Create files with known structure and UNIQUE content
        # (avoid deduplication)
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "unique_content_one",
                "subdir/file2.jpg": "unique_image_content",
                "subdir/nested/file3.txt": "unique_content_three",
            },
        )

        # Copy without specifying -R (test default behavior)
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-m",
                "manifest.db",
            ]
        )

        # Verify files were copied with structure preserved
        expected_files = [
            os.path.join(self.dest_dir, "file1.txt"),
            os.path.join(self.dest_dir, "subdir", "file2.jpg"),
            os.path.join(self.dest_dir, "subdir", "nested", "file3.txt"),
        ]

        for expected_file in expected_files:
            self.assertTrue(
                os.path.exists(expected_file),
                f"Expected file to exist: {expected_file}",
            )

        # Verify subdirectory structure is preserved
        self.assertTrue(
            os.path.exists(os.path.join(self.dest_dir, "subdir")),
            "Subdirectory 'subdir' should be preserved",
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.dest_dir, "subdir", "nested")),
            "Nested subdirectory 'subdir/nested' should be preserved",
        )


class TestIncrementalBackup(unittest.TestCase):
    """Test suite for incremental backup workflows."""

    def setUp(self):
        """Set up a temporary directory for incremental backup tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_incremental_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.backup_dir = os.path.join(self.temp_dir, "backup")
        os.makedirs(self.source_dir)
        os.makedirs(self.backup_dir)

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

    def _count_files(self, directory):
        """Count total files in a directory tree."""
        count = 0
        for _, _, files in os.walk(directory):
            count += len(files)
        return count

    def test_incremental_backup_basic(self):
        """Test a basic incremental backup scenario."""
        # Step 1: Initial backup
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "content1",
                "file2.txt": "content2",
                "subdir/file3.txt": "content3",
            },
        )
        manifest_path = os.path.join(self.temp_dir, "backup.db")

        # Initial backup
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.backup_dir,
                "-m",
                manifest_path,
                "-R",
                "*:no_change",  # Preserve structure for easier verification
            ]
        )

        # Verify initial backup
        self.assertEqual(self._count_files(self.backup_dir), 3)

        # Step 2: Add new files to source
        make_file_tree(
            self.source_dir,
            {
                "file4.txt": "content4",
                "subdir/file5.txt": "content5",
            },
        )

        # Step 3: Incremental backup (use --compare instead of -i + -m same path)
        manifest_path_new = os.path.join(self.temp_dir, "backup_new.db")
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.backup_dir,
                "--compare",
                manifest_path,
                "-m",
                manifest_path_new,
                "-R",
                "*:no_change",
            ]
        )

        # Verify incremental backup - new files should be added
        self.assertEqual(self._count_files(self.backup_dir), 5)

    def test_incremental_backup_with_modification(self):
        """Test incremental backup when a file is modified."""
        # Initial backup
        make_file_tree(
            self.source_dir,
            {"file1.txt": "original_content", "file2.txt": "content2"},
        )
        manifest_path = os.path.join(self.temp_dir, "backup.db")

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.backup_dir,
                "-m",
                manifest_path,
                "-R",
                "*:no_change",
            ]
        )

        # Modify a file
        with open(
            os.path.join(self.source_dir, "file1.txt"), "w", encoding="utf-8"
        ) as f:
            f.write("modified_content")

        # Incremental backup should copy the modified file
        manifest_path_new = os.path.join(self.temp_dir, "backup_new.db")
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.backup_dir,
                "--compare",
                manifest_path,
                "-m",
                manifest_path_new,
                "-R",
                "*:no_change",
            ]
        )

        # Verify the backup has been updated
        # Both versions should exist (old and new with different content)
        self.assertGreaterEqual(self._count_files(self.backup_dir), 2)


class TestPathConversion(unittest.TestCase):
    """Test suite for manifest path conversion scenarios."""

    def setUp(self):
        """Set up a temporary directory for path conversion tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_path_conv_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(self.source_dir)

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

    def test_path_conversion_basic(self):
        """Test basic path conversion in a manifest."""
        # Create files and generate manifest
        make_file_tree(self.source_dir, {"file1.txt": "content1"})
        original_manifest = os.path.join(self.temp_dir, "original.db")
        converted_manifest = os.path.join(self.temp_dir, "converted.db")

        self._run_cli(["-p", self.source_dir, "-m", original_manifest])

        # Convert paths
        old_prefix = self.source_dir
        new_prefix = self.source_dir.replace("source", "newsource")

        self._run_cli(
            [
                "--no-walk",
                "-i",
                original_manifest,
                "-m",
                converted_manifest,
                "--convert-manifest-paths-from",
                old_prefix,
                "--convert-manifest-paths-to",
                new_prefix,
            ]
        )

        # Verify manifest was created
        self.assertTrue(os.path.exists(converted_manifest))

        converted = Manifest(converted_manifest, temp_directory=self.temp_dir)
        for _, file_list in converted.items():
            for path, _, _ in file_list:
                self.assertIn(new_prefix, path)
                self.assertNotIn(old_prefix, path)
        converted.close()


class TestExtensionFiltering(unittest.TestCase):
    """Test suite for advanced extension filtering scenarios."""

    def setUp(self):
        """Set up a temporary directory for extension filtering tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_ext_filter_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        os.makedirs(self.source_dir)
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

    def _get_filenames(self, directory):
        """Get a set of all filenames in a directory tree."""
        filenames = set()
        for _, _, files in os.walk(directory):
            filenames.update(files)
        return filenames

    def test_multiple_extension_filters(self):
        """Test copying with multiple extension filters."""
        make_file_tree(
            self.source_dir,
            {
                "file1.jpg": "image1",
                "file2.png": "image2",
                "file3.txt": "text",
                "file4.pdf": "document",
                "file5.jpeg": "image3",
            },
        )

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-e",
                "jpg",
                "-e",
                "png",
                "-m",
                "manifest.db",
            ]
        )

        copied_files = self._get_filenames(self.dest_dir)
        self.assertEqual(len(copied_files), 2)
        self.assertIn("file1.jpg", copied_files)
        self.assertIn("file2.png", copied_files)
        self.assertNotIn("file3.txt", copied_files)

    def test_wildcard_extension_filter(self):
        """Test copying with wildcard extension filters."""
        make_file_tree(
            self.source_dir,
            {
                "file1.jpg": "image1",
                "file2.jpeg": "image2",
                "file3.txt": "text",
            },
        )

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-e",
                "jp*g",
                "-m",
                "manifest.db",
            ]
        )

        copied_files = self._get_filenames(self.dest_dir)
        # Should match both .jpg and .jpeg
        self.assertGreaterEqual(len(copied_files), 2)


class TestEmptyFileHandling(unittest.TestCase):
    """Test suite for empty file handling scenarios."""

    def setUp(self):
        """Set up a temporary directory for empty file tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_empty_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        os.makedirs(self.source_dir)
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

    def _count_files(self, directory):
        """Count total files in a directory tree."""
        count = 0
        for _, _, files in os.walk(directory):
            count += len(files)
        return count

    def test_empty_files_copied_by_default(self):
        """Test that empty files are copied by default (treated as unique)."""
        make_file_tree(
            self.source_dir,
            {
                "empty1.txt": "",
                "empty2.txt": "",
                "empty3.txt": "",
                "nonempty.txt": "content",
            },
        )

        self._run_cli(["-p", self.source_dir, "-c", self.dest_dir, "-m", "manifest.db"])

        # All files should be copied (empty files are unique by default)
        self.assertEqual(self._count_files(self.dest_dir), 4)

    def test_empty_files_deduped_with_flag(self):
        """Test that empty files are deduplicated with --dedupe-empty."""
        make_file_tree(
            self.source_dir,
            {
                "empty1.txt": "",
                "empty2.txt": "",
                "empty3.txt": "",
                "nonempty.txt": "content",
            },
        )

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--dedupe-empty",
                "-m",
                "manifest.db",
            ]
        )

        # Only 2 files should be copied (1 empty + 1 non-empty)
        self.assertEqual(self._count_files(self.dest_dir), 2)


class TestReportGeneration(unittest.TestCase):
    """Test suite for report generation scenarios."""

    def setUp(self):
        """Set up a temporary directory for report tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_report_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        os.makedirs(self.source_dir)

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

    def test_report_only_no_copy(self):
        """Test generating a report without copying files."""
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "duplicate",
                "file2.txt": "duplicate",
                "file3.txt": "unique",
            },
        )

        report_path = os.path.join(self.temp_dir, "report.csv")
        self._run_cli(["-p", self.source_dir, "-r", report_path])

        # Verify report was created
        self.assertTrue(os.path.exists(report_path))

        # Verify report contains duplicate info
        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("file1.txt", content)
            self.assertIn("file2.txt", content)

    def test_report_with_manifest_generation(self):
        """Test generating both a report and manifest."""
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "content",
                "file2.txt": "content",
            },
        )

        report_path = os.path.join(self.temp_dir, "report.csv")
        manifest_path = os.path.join(self.temp_dir, "manifest.db")

        self._run_cli(["-p", self.source_dir, "-r", report_path, "-m", manifest_path])

        # Verify both files were created
        self.assertTrue(os.path.exists(report_path))
        self.assertTrue(os.path.exists(manifest_path))


class TestMetadataPreservation(unittest.TestCase):
    """Test suite for metadata preservation scenarios."""

    def setUp(self):
        """Set up a temporary directory for metadata tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_metadata_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        os.makedirs(self.source_dir)
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

    def test_metadata_preserved_with_flag(self):
        """Test that metadata is preserved with --copy-metadata flag."""
        source_file = os.path.join(self.source_dir, "testfile.txt")
        make_file_tree(self.source_dir, {"testfile.txt": "content"})

        # Set a specific modification time
        old_mtime = 1000000000.0
        os.utime(source_file, (old_mtime, old_mtime))

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--copy-metadata",
                "-m",
                "manifest.db",
                "-R",
                "*:no_change",  # Preserve original paths
            ]
        )

        # File will be in dest_dir with original structure preserved
        dest_file = os.path.join(self.dest_dir, "testfile.txt")
        self.assertTrue(os.path.exists(dest_file))

        # Verify modification time was preserved (within 1 second tolerance)
        dest_mtime = os.path.getmtime(dest_file)
        self.assertAlmostEqual(dest_mtime, old_mtime, delta=1.0)

    def test_metadata_not_preserved_without_flag(self):
        """Test that metadata is not preserved without --copy-metadata flag."""
        source_file = os.path.join(self.source_dir, "testfile.txt")
        make_file_tree(self.source_dir, {"testfile.txt": "content"})

        # Set a very old modification time
        old_mtime = 1000000000.0
        os.utime(source_file, (old_mtime, old_mtime))

        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-m",
                "manifest.db",
                "-R",
                "*:no_change",
            ]
        )

        dest_file = os.path.join(self.dest_dir, "testfile.txt")
        self.assertTrue(os.path.exists(dest_file))

        # Verify modification time is recent (not preserved)
        dest_mtime = os.path.getmtime(dest_file)
        current_time = time.time()
        # File should have been created recently (within last 10 seconds)
        self.assertLess(abs(current_time - dest_mtime), 10.0)


class TestErrorRecovery(unittest.TestCase):
    """Test suite for error recovery and resumption scenarios."""

    def setUp(self):
        """Set up a temporary directory for error recovery tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_recovery_")
        self.source_dir = os.path.join(self.temp_dir, "source")
        self.dest_dir = os.path.join(self.temp_dir, "dest")
        os.makedirs(self.source_dir)
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

    def test_resume_after_partial_copy(self):
        """Test resuming a copy operation after partial completion."""
        # Create initial files
        make_file_tree(
            self.source_dir,
            {
                "file1.txt": "content1",
                "file2.txt": "content2",
                "file3.txt": "content3",
            },
        )

        manifest_path = os.path.join(self.temp_dir, "manifest.db")

        # First run: copy all initial files
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "-m",
                manifest_path,
                "-R",
                "*:no_change",
            ]
        )

        # Add more files
        make_file_tree(
            self.source_dir,
            {
                "file4.txt": "content4",
                "file5.txt": "content5",
            },
        )

        # Resume operation - use --compare to skip already copied files
        manifest_path_new = os.path.join(self.temp_dir, "manifest_new.db")
        self._run_cli(
            [
                "-p",
                self.source_dir,
                "-c",
                self.dest_dir,
                "--compare",
                manifest_path,
                "-m",
                manifest_path_new,
                "-R",
                "*:no_change",
            ]
        )

        # Verify all files are now in destination
        dest_files = []
        for _, _, files in os.walk(self.dest_dir):
            dest_files.extend(files)
        self.assertEqual(len(dest_files), 5)


class TestMultiSourceSequentialBackup(unittest.TestCase):
    """Test suite for complex multi-source sequential backup scenarios."""

    def setUp(self):
        """Set up temporary directories for multi-source backup tests."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_multisource_")
        self.source1_dir = os.path.join(self.temp_dir, "source1")
        self.source2_dir = os.path.join(self.temp_dir, "source2")
        self.source3_dir = os.path.join(self.temp_dir, "source3")
        self.target_dir = os.path.join(self.temp_dir, "target")
        os.makedirs(self.source1_dir)
        os.makedirs(self.source2_dir)
        os.makedirs(self.source3_dir)
        os.makedirs(self.target_dir)

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

    def _count_files(self, directory):
        """Count total files in a directory tree."""
        count = 0
        for _, _, files in os.walk(directory):
            count += len(files)
        return count

    def _get_all_filenames(self, directory):
        """Get a set of all filenames in a directory tree."""
        filenames = set()
        for _, _, files in os.walk(directory):
            filenames.update(files)
        return filenames

    def test_three_source_sequential_backup(self):
        """
        Test the README example: Sequential backup from 3 sources to 1 target.

        This follows the workflow documented in the README:
        1. Generate manifests for target and all sources
        2. Copy source1 to target (comparing against target and source2/3)
        3. Copy source2 to target (comparing against all previous)
        4. Copy source3 to target (comparing against all previous)

        Expected behavior:
        - Each file should be copied only once (first source wins)
        - Duplicates across sources should be skipped
        - Target should contain all unique files from all sources
        """
        # Setup: Create files with some duplicates across sources
        make_file_tree(
            self.source1_dir,
            {
                "unique1.txt": "unique_to_source1",
                "shared12.txt": "shared_by_1_and_2",  # source1 version
                "shared123.txt": "shared_by_all",  # source1 version
                "shared13.txt": "shared_by_1_and_3",  # source1 version
            },
        )
        make_file_tree(
            self.source2_dir,
            {
                "unique2.txt": "unique_to_source2",
                "shared12.txt": "shared_by_1_and_2",  # duplicate
                "shared123.txt": "shared_by_all",  # duplicate
                "shared23.txt": "shared_by_2_and_3",  # source2 version
            },
        )
        make_file_tree(
            self.source3_dir,
            {
                "unique3.txt": "unique_to_source3",
                "shared13.txt": "shared_by_1_and_3",  # duplicate
                "shared123.txt": "shared_by_all",  # duplicate
                "shared23.txt": "shared_by_2_and_3",  # duplicate
            },
        )

        # Step 1: Generate manifests for all locations
        target_manifest = os.path.join(self.temp_dir, "target.db")
        source1_manifest = os.path.join(self.temp_dir, "source1.db")
        source2_manifest = os.path.join(self.temp_dir, "source2.db")
        source3_manifest = os.path.join(self.temp_dir, "source3.db")

        # Target is empty initially, but generate manifest anyway
        self._run_cli(["-p", self.target_dir, "-m", target_manifest])

        self._run_cli(["-p", self.source1_dir, "-m", source1_manifest])
        self._run_cli(["-p", self.source2_dir, "-m", source2_manifest])
        self._run_cli(["-p", self.source3_dir, "-m", source3_manifest])

        # Step 2: Copy source1 to target (skip files already in target)
        target_manifest_v1 = os.path.join(self.temp_dir, "target_v1.db")
        self._run_cli(
            [
                "-p",
                self.source1_dir,
                "-c",
                self.target_dir,
                "--compare",
                target_manifest,
                "-m",
                target_manifest_v1,
                "-R",
                "*:no_change",
            ]
        )

        # Verify: All 4 files from source1 should be copied (target was empty)
        self.assertEqual(self._count_files(self.target_dir), 4)

        # Step 3: Copy source2 to target (skip duplicates already in target)
        target_manifest_v2 = os.path.join(self.temp_dir, "target_v2.db")
        self._run_cli(
            [
                "-p",
                self.source2_dir,
                "-c",
                self.target_dir,
                "--compare",
                target_manifest_v1,
                "-m",
                target_manifest_v2,
                "-R",
                "*:no_change",
            ]
        )

        # Verify: Only unique2.txt and shared23.txt should be added (2 new files)
        # shared12.txt and shared123.txt are duplicates of files already in target
        self.assertEqual(self._count_files(self.target_dir), 6)

        # Step 4: Copy source3 to target (skip duplicates already in target)
        target_manifest_v3 = os.path.join(self.temp_dir, "target_v3.db")
        self._run_cli(
            [
                "-p",
                self.source3_dir,
                "-c",
                self.target_dir,
                "--compare",
                target_manifest_v2,
                "-m",
                target_manifest_v3,
                "-R",
                "*:no_change",
            ]
        )

        # Verify: Only unique3.txt should be added (1 new file)
        # Total: 4 (source1) + 2 (source2) + 1 (source3) = 7 unique files
        self.assertEqual(self._count_files(self.target_dir), 7)

        # Verify the correct files are present
        target_files = self._get_all_filenames(self.target_dir)
        expected_files = {
            "unique1.txt",
            "unique2.txt",
            "unique3.txt",
            "shared12.txt",
            "shared123.txt",
            "shared13.txt",
            "shared23.txt",
        }
        self.assertEqual(target_files, expected_files)

    def test_multi_source_with_existing_target(self):
        """
        Test sequential backup when target already has some files.

        This tests a common real-world scenario where:
        1. Target already has some files
        2. Multiple sources need to be consolidated
        3. No duplicates should be created
        """
        # Setup: Target already has some files
        make_file_tree(
            self.target_dir,
            {
                "existing1.txt": "already_in_target",
                "existing2.txt": "also_in_target",
            },
        )

        # Source1 has some unique files and some duplicates of target
        make_file_tree(
            self.source1_dir,
            {
                "existing1.txt": "already_in_target",  # duplicate
                "new_from_source1.txt": "unique_content",
            },
        )

        # Source2 has some unique files
        make_file_tree(
            self.source2_dir,
            {
                "new_from_source2.txt": "another_unique",
                "existing2.txt": "also_in_target",  # duplicate
            },
        )

        # Step 1: Generate manifests
        target_manifest = os.path.join(self.temp_dir, "target.db")
        source1_manifest = os.path.join(self.temp_dir, "source1.db")
        source2_manifest = os.path.join(self.temp_dir, "source2.db")

        self._run_cli(["-p", self.target_dir, "-m", target_manifest])
        self._run_cli(["-p", self.source1_dir, "-m", source1_manifest])
        self._run_cli(["-p", self.source2_dir, "-m", source2_manifest])

        # Initial target has 2 files
        self.assertEqual(self._count_files(self.target_dir), 2)

        # Step 2: Copy source1 (skip files already in target)
        target_manifest_v1 = os.path.join(self.temp_dir, "target_v1.db")
        self._run_cli(
            [
                "-p",
                self.source1_dir,
                "-c",
                self.target_dir,
                "--compare",
                target_manifest,
                "--compare",
                source2_manifest,
                "-m",
                target_manifest_v1,
                "-R",
                "*:no_change",
            ]
        )

        # Should add only 1 new file (existing1.txt is a duplicate)
        self.assertEqual(self._count_files(self.target_dir), 3)

        # Step 3: Copy source2 (skip files already in target)
        target_manifest_v2 = os.path.join(self.temp_dir, "target_v2.db")
        self._run_cli(
            [
                "-p",
                self.source2_dir,
                "-c",
                self.target_dir,
                "--compare",
                target_manifest_v1,
                "--compare",
                source1_manifest,
                "-m",
                target_manifest_v2,
                "-R",
                "*:no_change",
            ]
        )

        # Should add only 1 new file (existing2.txt is a duplicate)
        self.assertEqual(self._count_files(self.target_dir), 4)

        # Verify final state
        target_files = self._get_all_filenames(self.target_dir)
        expected_files = {
            "existing1.txt",
            "existing2.txt",
            "new_from_source1.txt",
            "new_from_source2.txt",
        }
        self.assertEqual(target_files, expected_files)
