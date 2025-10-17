"""Tets --no-walk functionality - confim operations work when suppling a manifest only."""

import io
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli
from dedupe_copy.disk_cache_dict import CacheDict


class TestNoWalk(unittest.TestCase):
    """Test --no-walk functionality"""

    def setUp(self):
        """Set up a test environment with pre-existing files and a manifest."""
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

        # Generate the manifest once for all tests
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "-p",
                self.files_dir,
                "-m",
                self.manifest_path,
            ],
        ):
            # Capture stdout to avoid polluting test output
            f = io.StringIO()
            with redirect_stdout(f):
                run_cli()

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
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "--delete",
                "-i",
                self.manifest_path,
                "-m",
                self.manifest_path + ".new",
                "--min-delete-size",
                "1",  # delete all duplicates
            ],
        ):
            run_cli()

        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 3)
        self.assertIn("e.txt", remaining_files)
        # Check first group of duplicates
        self.assertTrue(("a.txt" in remaining_files) ^ ("b.txt" in remaining_files))
        # Check second group of duplicates
        self.assertTrue(("c.txt" in remaining_files) ^ ("d.txt" in remaining_files))

    def test_no_walk_report(self):
        """--no-walk with -r generates a report"""
        report_path = os.path.join(self.root, "report.csv")

        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "-r",
                report_path,
                "-i",
                self.manifest_path,
            ],
        ):
            run_cli()

        self.assertTrue(os.path.exists(report_path))
        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("a.txt", content)
            self.assertIn("b.txt", content)
            self.assertIn("c.txt", content)
            self.assertIn("d.txt", content)
            self.assertNotIn("e.txt", content)

    def test_no_walk_delete_dry_run_min_size(self):
        """--no-walk --delete --dry-run with --min-delete-size"""
        f = io.StringIO()
        with redirect_stdout(f):
            with patch(
                "sys.argv",
                [
                    "dedupecopy",
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
            ):
                run_cli()

        output = f.getvalue()

        self.assertIn("DRY RUN: Would have started deletion of 1 files.", output)
        self.assertIn("[DRY RUN] Would delete", output)
        self.assertIn("Skipping deletion of ", output)
        self.assertIn("with size 3 bytes", output)

        remaining_files = os.listdir(self.files_dir)
        self.assertEqual(len(remaining_files), 5)

    def test_no_walk_does_not_modify_read_manifest(self):
        """--no-walk should not modify the .read manifest file, even if inconsistent."""

        read_manifest_path = self.manifest_path + ".read"

        # 1. Create inconsistent manifest by removing an entry from the .read file
        read_manifest = CacheDict(db_file=read_manifest_path)
        read_manifest.load()
        self.assertEqual(len(read_manifest), 5, "Initial manifest should have 5 files.")

        key_to_remove = os.path.join(self.files_dir, "a.txt")
        del read_manifest[key_to_remove]
        read_manifest.save()
        read_manifest.close()

        # 2. Verify inconsistency and get count
        read_manifest_reloaded = CacheDict(db_file=read_manifest_path)
        read_manifest_reloaded.load()
        inconsistent_count = len(read_manifest_reloaded)
        self.assertEqual(
            inconsistent_count, 4, "Manifest should now be inconsistent with 4 files."
        )
        read_manifest_reloaded.close()

        # 3. Run with --no-walk, which will trigger a save operation
        with patch(
            "sys.argv",
            [
                "dedupecopy",
                "--no-walk",
                "-i",
                self.manifest_path,
                "-m",
                self.manifest_path + ".new",
            ],
        ):
            f = io.StringIO()
            with redirect_stdout(f):
                run_cli()

        # 4. Check that the file count has NOT changed
        final_read_manifest = CacheDict(db_file=read_manifest_path)
        final_read_manifest.load()
        final_count = len(final_read_manifest)
        final_read_manifest.close()

        self.assertEqual(
            final_count,
            inconsistent_count,
            "The number of files in .read manifest should not change on a --no-walk run.",
        )
