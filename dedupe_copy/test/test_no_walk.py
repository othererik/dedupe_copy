"""Tets --no-walk functionality - confim operations work when suppling a manifest only."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli


class TestNoWalk(unittest.TestCase):
    """Test --no-walk functionality"""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.files_dir = os.path.join(self.root, "files")
        os.makedirs(self.files_dir)

    def tearDown(self):
        shutil.rmtree(self.root)

    def _create_file(self, name, content):
        """Creates a file with specified name and content in the files_dir"""
        path = os.path.join(self.files_dir, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def test_no_walk_delete(self):
        """--no-walk with --delete deletes files"""
        self._create_file("a.txt", "content1")
        self._create_file("b.txt", "content1")
        self._create_file("c.txt", "content2")

        manifest_path = os.path.join(self.root, "manifest.db")

        # 1. Generate manifest
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "-p",
                self.files_dir,
                "-m",
                manifest_path,
            ],
        ):
            run_cli()

        # 2. Run with --no-walk and --delete
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "--delete",
                "-i",
                manifest_path,
                "--min-delete-size",
                "1",
            ],
        ):
            run_cli()

        # One of the duplicates should be deleted
        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 2)
        self.assertIn("c.txt", remaining_files)
        # only one of a or b should exist
        self.assertTrue(
            ("a.txt" in remaining_files and "b.txt" not in remaining_files)
            or ("b.txt" in remaining_files and "a.txt" not in remaining_files)
        )

    def test_no_walk_report(self):
        """--no-walk with -r generates a report"""
        self._create_file("a.txt", "content1")
        self._create_file("b.txt", "content1")
        self._create_file("c.txt", "content2")

        manifest_path = os.path.join(self.root, "manifest.db")
        report_path = os.path.join(self.root, "report.csv")

        # 1. Generate manifest
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "-p",
                self.files_dir,
                "-m",
                manifest_path,
            ],
        ):
            run_cli()

        # 2. Run with --no-walk and -r
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "-r",
                report_path,
                "-i",
                manifest_path,
            ],
        ):
            run_cli()

        self.assertTrue(os.path.exists(report_path))
        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("a.txt", content)
            self.assertIn("b.txt", content)
            self.assertNotIn("c.txt", content)
