"""Tests for the manifest explorer CLI."""

import io
import os
import tempfile
import unittest
from unittest.mock import patch

from dedupe_copy.bin.manifest_explorer_cli import ManifestExplorer
from dedupe_copy.manifest import Manifest


class TestManifestExplorer(unittest.TestCase):
    """Tests for the ManifestExplorer class."""

    def setUp(self):
        """Set up a temporary directory and create a dummy manifest."""
        self.temp_dir = (
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )
        self.addCleanup(self.temp_dir.cleanup)
        self.test_dir = self.temp_dir.name
        self.manifest_path = os.path.join(self.test_dir, "test_manifest.db")

        # Create a dummy manifest
        manifest = Manifest(manifest_paths=None, save_path=self.manifest_path)
        manifest["hash1"] = [("file1.txt", 10, 12345)]
        manifest["hash2"] = [("file2.txt", 20, 67890)]
        manifest.save()
        manifest.close()

        self.explorer = ManifestExplorer()

    def tearDown(self):
        """Clean up the dummy manifest."""
        if self.explorer.manifest:
            self.explorer.manifest.close()
        # The temp directory and its contents will be cleaned up by addCleanup

    def test_load_manifest(self):
        """Test loading a manifest."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd(f"load {self.manifest_path}")
            self.assertIn("loaded successfully", fake_out.getvalue())
        self.assertIsNotNone(self.explorer.manifest)

    def test_info(self):
        """Test the info command."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("info")
            output = fake_out.getvalue()
            self.assertIn("Hashes: 2", output)
            self.assertIn("Files: 2", output)

    def test_list(self):
        """Test the list command."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("list")
            output = fake_out.getvalue()
            self.assertIn("hash1", output)
            self.assertIn("file1.txt", output)
            self.assertIn("hash2", output)
            self.assertIn("file2.txt", output)

    def test_find(self):
        """Test the find command."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find hash1")
            self.assertIn("Found hash: hash1", fake_out.getvalue())
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find file1.txt")
            self.assertIn("Found file: file1.txt", fake_out.getvalue())

    def test_exit(self):
        """Test the exit command."""
        self.assertTrue(self.explorer.onecmd("exit"))


if __name__ == "__main__":
    unittest.main()
