"""Tests for the manifest explorer CLI."""

import io
import os
import tempfile
import unittest
from unittest.mock import patch

from dedupe_copy.bin.manifest_explorer_cli import ManifestExplorer, main
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

    def test_load_manifest_not_found(self):
        """Test loading a non-existent manifest."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("load non_existent_file.db")
            self.assertIn("Error: Manifest file not found", fake_out.getvalue())
        self.assertIsNone(self.explorer.manifest)

    def test_load_manifest_no_arg(self):
        """Test the load command with no argument."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("load")
            self.assertIn("Please provide the path", fake_out.getvalue())

    def test_info(self):
        """Test the info command."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("info")
            output = fake_out.getvalue()
            self.assertIn("Hashes: 2", output)
            self.assertIn("Files: 2", output)

    def test_info_no_manifest(self):
        """Test the info command with no manifest loaded."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("info")
            self.assertIn("No manifest loaded", fake_out.getvalue())

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

    def test_list_no_manifest(self):
        """Test the list command with no manifest loaded."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("list")
            self.assertIn("No manifest loaded", fake_out.getvalue())

    def test_list_invalid_limit(self):
        """Test the list command with an invalid limit."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("list abc")
            self.assertIn("Invalid limit", fake_out.getvalue())

    def test_find(self):
        """Test the find command."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find hash1")
            self.assertIn("Found hash: hash1", fake_out.getvalue())
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find file1.txt")
            self.assertIn("Found file: file1.txt", fake_out.getvalue())

    def test_find_no_manifest(self):
        """Test the find command with no manifest loaded."""
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find foo")
            self.assertIn("No manifest loaded", fake_out.getvalue())

    def test_find_no_arg(self):
        """Test the find command with no argument."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find")
            self.assertIn("Please provide a search query", fake_out.getvalue())

    def test_find_not_found(self):
        """Test the find command with a non-existent query."""
        self.explorer.onecmd(f"load {self.manifest_path}")
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            self.explorer.onecmd("find non_existent")
            self.assertIn("No matching hash or file found", fake_out.getvalue())

    def test_exit(self):
        """Test the exit command."""
        self.assertTrue(self.explorer.onecmd("exit"))

    @patch("dedupe_copy.bin.manifest_explorer_cli.ManifestExplorer.cmdloop")
    def test_main_keyboard_interrupt(self, mock_cmdloop):
        """Test the main function with a KeyboardInterrupt."""
        mock_cmdloop.side_effect = KeyboardInterrupt
        with patch("sys.stdout", new=io.StringIO()) as fake_out:
            main()
            self.assertIn("Exiting", fake_out.getvalue())


if __name__ == "__main__":
    unittest.main()
