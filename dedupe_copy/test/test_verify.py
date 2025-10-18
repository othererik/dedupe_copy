"""Basic tests for the verify_manifest_fs function."""

import os
import unittest

from dedupe_copy.test import utils
from dedupe_copy.core import verify_manifest_fs
from dedupe_copy.manifest import Manifest


class TestVerify(unittest.TestCase):
    """Tests for verify_manifest_fs function."""

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("verify")
        self.manifest_path = os.path.join(self.temp_dir, "manifest.dict")
        self.manifest = Manifest(
            None, save_path=self.manifest_path, temp_directory=self.temp_dir
        )

        self.file_size = 100
        # use_unique_files=True will create files with different content and thus different hashes
        self.file_list = utils.make_file_tree(
            root=self.temp_dir,
            file_spec=5,
            file_size=self.file_size,
            use_unique_files=True,
        )

        md5data = {}
        for path, file_hash, mtime in self.file_list:
            if file_hash not in md5data:
                md5data[file_hash] = []
            md5data[file_hash].append([path, self.file_size, mtime])

        self.manifest.md5_data.update(md5data)
        self.manifest.save()

    def tearDown(self):
        """Remove temporary directory and all test files"""
        del self.manifest
        utils.remove_dir(self.temp_dir)

    def test_verify_success(self):
        """Test successful verification of a manifest."""
        result = verify_manifest_fs(self.manifest)
        self.assertTrue(result)

    def test_verify_missing_file(self):
        """Test verification with a missing file."""
        # Get a file to remove
        file_to_remove, _, _ = self.file_list[0]
        os.remove(file_to_remove)

        with self.assertLogs("dedupe_copy.core", level="ERROR") as cm:
            result = verify_manifest_fs(self.manifest)
            self.assertFalse(result)
            self.assertTrue(
                any(
                    f"VERIFY FAILED: File not found: {file_to_remove}" in s
                    for s in cm.output
                )
            )

    def test_verify_size_mismatch(self):
        """Test verification with a file that has a different size."""
        # Get a file to modify
        file_to_modify, _, _ = self.file_list[0]
        original_size = self.file_size
        with open(file_to_modify, "a", encoding="utf-8") as f:
            f.write("add some data")

        new_size = os.path.getsize(file_to_modify)

        with self.assertLogs("dedupe_copy.core", level="ERROR") as cm:
            result = verify_manifest_fs(self.manifest)
            self.assertFalse(result)
            self.assertTrue(
                any(
                    f"VERIFY FAILED: Size mismatch for {file_to_modify}: "
                    f"expected {original_size}, got {new_size}" in s
                    for s in cm.output
                )
            )
