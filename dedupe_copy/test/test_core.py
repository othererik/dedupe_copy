"""Tests for dedupe_copy.core."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from dedupe_copy.core import info_parser, run_dupe_copy
from dedupe_copy.manifest import Manifest


class TestInfoParser(unittest.TestCase):
    """Tests for dedupe_copy.core.info_parser."""

    @patch("dedupe_copy.core.datetime")
    def test_info_parser_handles_timestamp_errors(self, mock_datetime):
        """Test that info_parser handles OverflowError when converting timestamps."""
        mock_datetime.datetime.fromtimestamp.side_effect = OverflowError(
            "mocked overflow error"
        )
        data = {"some_md5": [["a/file/path", 100, 1234567890]]}
        results = list(info_parser(data))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][2], "Unknown")


class TestRunDupeCopy(unittest.TestCase):
    """Tests for the main run_dupe_copy function."""

    def setUp(self):
        """Set up a temporary directory for tests."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.test_dir)

    def test_delete_without_manifest_out_does_not_overwrite_input(self):
        """Verify that --delete does not overwrite the input manifest."""
        # 1. Setup: Create a directory with duplicate files
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(src_dir)
        file1_path = os.path.join(src_dir, "file1.txt")
        file2_path = os.path.join(src_dir, "file2.txt")
        with open(file1_path, "w", encoding="utf-8") as f:
            f.write("duplicate content")
        with open(file2_path, "w", encoding="utf-8") as f:
            f.write("duplicate content")

        # 2. Create an initial manifest programmatically
        manifest_in_path = os.path.join(self.test_dir, "manifest.db")
        manifest = Manifest(
            None, save_path=manifest_in_path, temp_directory=self.test_dir
        )
        the_hash = "d34861214a1419720453305a16027201"  # md5 of "duplicate content"
        manifest[the_hash] = [
            [file1_path, 17, os.path.getmtime(file1_path)],
            [file2_path, 17, os.path.getmtime(file2_path)],
        ]
        manifest.read_sources[file1_path] = None
        manifest.read_sources[file2_path] = None
        manifest.save()
        manifest.close()

        # 3. Run the delete operation without specifying an output manifest
        # This will delete file2.txt because of sort order
        run_dupe_copy(
            manifests_in_paths=[manifest_in_path],
            delete_duplicates=True,
            no_walk=True,
        )

        # 4. Assert that the input manifest was NOT modified
        self.assertTrue(os.path.exists(file1_path))
        self.assertFalse(os.path.exists(file2_path), "File should have been deleted")
        manifest_after = Manifest(manifest_in_path, temp_directory=self.test_dir)
        self.assertEqual(
            len(manifest_after.md5_data), 1, "Manifest should still contain the hash."
        )
        self.assertEqual(
            len(manifest_after.md5_data[the_hash]),
            2,
            "Manifest file list for hash should be unchanged.",
        )
        manifest_after.close()

    def test_delete_with_manifest_out_saves_updated_manifest(self):
        """Verify --delete with --manifest-dump-path saves a correct, updated manifest."""
        # 1. Setup: Create a directory with duplicate files
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(src_dir)
        file1_path = os.path.join(src_dir, "file1.txt")
        file2_path = os.path.join(src_dir, "file2.txt")
        with open(file1_path, "w", encoding="utf-8") as f:
            f.write("duplicate content")
        shutil.copy(file1_path, file2_path)

        # 2. Run dedupe to generate an initial manifest
        manifest_in_path = os.path.join(self.test_dir, "manifest.db")
        run_dupe_copy(read_from_path=[src_dir], manifest_out_path=manifest_in_path)

        # 3. Run the delete operation with an output manifest
        manifest_out_path = os.path.join(self.test_dir, "manifest_after_delete.db")
        run_dupe_copy(
            manifests_in_paths=[manifest_in_path],
            manifest_out_path=manifest_out_path,
            delete_duplicates=True,
            no_walk=True,
        )

        # 4. Assert that the output manifest correctly reflects the deletion
        self.assertTrue(os.path.exists(file1_path))
        self.assertFalse(
            os.path.exists(file2_path), "Duplicate file should have been deleted"
        )

        manifest_after = Manifest(manifest_out_path, temp_directory=self.test_dir)
        the_hash = "e7faa48ad4fcab277902b749a7a91353"  # md5 of "duplicate content"

        # This is the core assertion for the bug
        self.assertIn(the_hash, manifest_after.md5_data)
        self.assertEqual(
            len(manifest_after.md5_data[the_hash]),
            1,
            "Manifest should only contain one file for the hash after deletion.",
        )
        self.assertEqual(
            manifest_after.md5_data[the_hash][0][0],
            file1_path,
            "The remaining file in the manifest should be file1.txt",
        )
        manifest_after.close()

    def test_delete_handles_os_error_and_preserves_manifest(self):
        """Verify that if a file fails to delete, it remains in the manifest."""
        # 1. Setup: Create a directory with duplicate files
        src_dir = os.path.join(self.test_dir, "src")
        os.makedirs(src_dir)
        file1_path = os.path.join(src_dir, "file1.txt")
        file2_path = os.path.join(src_dir, "file2.txt")
        with open(file1_path, "w", encoding="utf-8") as f:
            f.write("duplicate content")
        shutil.copy(file1_path, file2_path)

        # 2. Run dedupe to generate an initial manifest
        manifest_in_path = os.path.join(self.test_dir, "manifest.db")
        run_dupe_copy(read_from_path=[src_dir], manifest_out_path=manifest_in_path)

        # 3. Mock os.remove in the context of the DeleteThread
        manifest_out_path = os.path.join(self.test_dir, "manifest_after_delete.db")
        with patch(
            "dedupe_copy.threads.os.remove", side_effect=OSError("Permission denied")
        ):
            run_dupe_copy(
                manifests_in_paths=[manifest_in_path],
                manifest_out_path=manifest_out_path,
                delete_duplicates=True,
                no_walk=True,
            )

        # 4. Assertions
        # The file that should have been deleted still exists on disk
        self.assertTrue(
            os.path.exists(file2_path),
            "File should not have been deleted due to mock error",
        )

        # Load the output manifest and check its contents
        manifest_after = Manifest(manifest_out_path, temp_directory=self.test_dir)
        the_hash = "e7faa48ad4fcab277902b749a7a91353"  # md5 of "duplicate content"

        # This is the core assertion that should fail with the current code
        self.assertIn(
            the_hash, manifest_after.md5_data, "Hash should be in the new manifest."
        )
        self.assertEqual(
            len(manifest_after.md5_data[the_hash]),
            2,
            "Manifest should still contain TWO files for the hash because deletion failed.",
        )
        manifest_after.close()
