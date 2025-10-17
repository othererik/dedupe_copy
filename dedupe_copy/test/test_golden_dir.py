"""Tests for the 'golden directory' backup use case."""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dedupe_copy.bin.dedupecopy_cli import run_cli


class TestGoldenDirUseCase(unittest.TestCase):
    """Tests for the 'golden directory' backup use case."""

    def setUp(self):
        # pylint: disable=consider-using-with
        self.temp_dir = tempfile.TemporaryDirectory()
        self.golden_dir = Path(self.temp_dir.name) / "golden"
        self.source_dir1 = Path(self.temp_dir.name) / "source1"
        self.source_dir2 = Path(self.temp_dir.name) / "source2"
        self.manifest_path = Path(self.temp_dir.name) / "golden_manifest.json"

        # Create directories
        self.golden_dir.mkdir()
        self.source_dir1.mkdir()
        self.source_dir2.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_golden_dir_backup(self):
        """Verify that only new files are copied to the golden directory."""
        # 1. Setup file trees manually
        # Golden directory has two files
        (self.golden_dir / "file_a.txt").write_text("content a")
        (self.golden_dir / "file_b.txt").write_text("content b")

        # Source 1 has a duplicate of file_a and a new file_c
        (self.source_dir1 / "file_a.txt").write_text("content a")
        (self.source_dir1 / "file_c.txt").write_text("content c")

        # Source 2 has a duplicate of file_b and a new file_d in a subfolder
        (self.source_dir2 / "subfolder").mkdir()
        (self.source_dir2 / "subfolder" / "file_b.txt").write_text("content b")
        (self.source_dir2 / "subfolder" / "file_d.txt").write_text("content d")

        # 2. Generate manifest for the golden directory by mocking sys.argv
        cli_args_manifest = [
            "dedupecopy",
            "--read-path",
            str(self.golden_dir),
            "--manifest-dump-path",
            str(self.manifest_path),
        ]
        with patch.object(sys, "argv", cli_args_manifest):
            run_cli()
        self.assertTrue(self.manifest_path.exists())

        # 3. Copy from sources to golden, using the manifest to avoid duplicates
        cli_args_copy = [
            "dedupecopy",
            "--read-path",
            str(self.source_dir1),
            "--read-path",
            str(self.source_dir2),
            "--compare",
            str(self.manifest_path),
            "--copy-path",
            str(self.golden_dir),
            "-R",
            "*:no_change",
        ]
        with patch.object(sys, "argv", cli_args_copy):
            run_cli()

        # 4. Verify the final state of the golden directory
        final_files = {
            os.path.normpath(str(p.relative_to(self.golden_dir)))
            for p in self.golden_dir.rglob("*")
            if p.is_file()
        }

        expected_files = {
            os.path.normpath("file_a.txt"),
            os.path.normpath("file_b.txt"),
            os.path.normpath("file_c.txt"),
            os.path.normpath("subfolder/file_d.txt"),
        }

        self.assertEqual(final_files, expected_files)

        # Also verify content to be extra sure the correct files were copied
        with open(self.golden_dir / "file_c.txt", "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "content c")
        with open(
            self.golden_dir / "subfolder" / "file_d.txt", "r", encoding="utf-8"
        ) as f:
            self.assertEqual(f.read(), "content d")

    def test_golden_dir_backup_with_delete(self):
        """Verify that source files are deleted after being copied or identified as duplicates."""
        # 1. Setup file trees manually
        (self.golden_dir / "file_a.txt").write_text("content a")
        (self.source_dir1 / "file_a.txt").write_text("content a")  # Duplicate
        (self.source_dir1 / "file_c.txt").write_text("content c")  # New file

        # 2. Generate manifest for the golden directory
        cli_args_manifest = [
            "dedupecopy",
            "--read-path",
            str(self.golden_dir),
            "--manifest-dump-path",
            str(self.manifest_path),
        ]
        with patch.object(sys, "argv", cli_args_manifest):
            run_cli()
        self.assertTrue(self.manifest_path.exists())

        # 3. Copy from source to golden with --delete-on-copy
        cli_args_copy_delete = [
            "dedupecopy",
            "--read-path",
            str(self.source_dir1),
            "--compare",
            str(self.manifest_path),
            "--copy-path",
            str(self.golden_dir),
            "--delete-on-copy",
            "-R",
            "*:no_change",
            # We need an output manifest for deletion to be allowed
            "--manifest-dump-path",
            f"{self.manifest_path}.new",
        ]
        with patch.object(sys, "argv", cli_args_copy_delete):
            run_cli()

        # 4. Verify the final state of the golden directory
        golden_files = {
            os.path.normpath(str(p.relative_to(self.golden_dir)))
            for p in self.golden_dir.rglob("*")
            if p.is_file()
        }
        expected_golden_files = {
            os.path.normpath("file_a.txt"),
            os.path.normpath("file_c.txt"),
        }
        self.assertEqual(golden_files, expected_golden_files)

        # 5. Verify that the source directory is now empty
        source_files = list(self.source_dir1.rglob("*"))
        self.assertEqual(len(source_files), 0, "Source directory should be empty")
