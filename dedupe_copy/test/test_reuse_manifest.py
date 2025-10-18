"""
Test for issue #32
https://github.com/othererik/dedupe_copy/issues/32
"""

import os
import sys
import shutil
import unittest
from unittest.mock import patch

from dedupe_copy.bin import dedupecopy_cli
from dedupe_copy.manifest import Manifest
from dedupe_copy.test import utils


class TestIssue32(unittest.TestCase):
    """
    Test for issue #32
    """

    def setUp(self):
        """Create temporary directory and test data with duplicates"""
        self.temp_dir = utils.make_temp_dir("issue32")
        self.src_dir = os.path.join(self.temp_dir, "src")
        self.manifest_path = os.path.join(self.temp_dir, "manifest.json")
        os.makedirs(self.src_dir)
        # Create 5 unique files
        utils.make_file_tree(self.src_dir, file_spec=5)
        # Create a subdirectory and copy the files to create duplicates
        dup_dir = os.path.join(self.src_dir, "dup")
        os.makedirs(dup_dir)
        for dirpath, _, filenames in os.walk(self.src_dir):
            if dup_dir in dirpath:
                continue
            for item in filenames:
                s = os.path.join(dirpath, item)
                # create the same subdirectory structure in dup_dir
                rel_path = os.path.relpath(dirpath, self.src_dir)
                d_dir = os.path.join(dup_dir, rel_path)
                if not os.path.exists(d_dir):
                    os.makedirs(d_dir)
                d = os.path.join(d_dir, item)
                shutil.copy2(s, d)

        # Run once to create the manifest
        with patch.object(
            sys, "argv", ["dedupecopy", "-p", self.src_dir, "-m", self.manifest_path]
        ):
            dedupecopy_cli.run_cli()

    def tearDown(self):
        """Remove temporary directory and all test files"""
        utils.remove_dir(self.temp_dir)

    @patch("dedupe_copy.threads._is_file_processing_required", return_value=True)
    def test_read_sources_duplicates_on_load_and_walk(
        self, mock_check
    ):  # pylint: disable=unused-argument
        """
        Test that running with -i and -p does not duplicate paths in memory
        by forcing files to be re-processed.
        """
        # There should be 10 total files (5 unique, 5 duplicates)
        manifest = Manifest(self.manifest_path, temp_directory=self.temp_dir)
        self.assertEqual(len(manifest.read_sources), 10)
        manifest.close()

        # Run again, forcing re-processing of all files
        with patch.object(
            sys,
            "argv",
            [
                "dedupecopy",
                "-p",
                self.src_dir,
                "-i",
                self.manifest_path,
                "-m",
                self.manifest_path + ".new",
            ],
        ):
            dedupecopy_cli.run_cli()

        # check the manifest again, the count should still be 10, not 20
        manifest2 = Manifest(self.manifest_path, temp_directory=self.temp_dir)
        self.assertEqual(len(manifest2.read_sources), 10)
        manifest2.close()
