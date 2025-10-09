"""Utility function tests for dedupe_copy."""

import unittest
import importlib
from unittest.mock import patch
from dedupe_copy.utils import format_error_message, clean_extensions
from dedupe_copy import utils as app_utils


class TestAppUtils(unittest.TestCase):
    """Tests for functions in dedupe_copy.utils"""

    def test_xxhash_import_warning(self):
        """Test that a warning is logged if xxhash is not installed."""
        # Mock the import of xxhash to simulate it not being installed
        original_import = __import__

        def import_mock(name, *args, **kwargs):
            if name == "xxhash":
                raise ImportError
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=import_mock):
            with self.assertLogs("dedupe_copy.utils", level="WARNING") as cm:
                importlib.reload(app_utils)
                self.assertEqual(len(cm.output), 1)
                self.assertIn("xxhash module not found", cm.output[0])
                self.assertIsNone(app_utils.xxhash)

        # Reload to restore the original state
        importlib.reload(app_utils)

    def test_format_error_message(self):
        """Test the format_error_message function with various error types."""
        test_path = "/fake/path/to/file.txt"

        # Test PermissionError
        perm_error = PermissionError("Permission denied")
        msg = format_error_message(test_path, perm_error)
        self.assertIn("Permission denied", msg)
        self.assertIn("Check file permissions", msg)

        # Test FileNotFoundError
        fnf_error = FileNotFoundError("No such file or directory")
        msg = format_error_message(test_path, fnf_error)
        self.assertIn("No such file", msg)
        self.assertIn("File may have been deleted", msg)

        # Test OSError (No space left)
        nospc_error = OSError(28, "No space left on device")
        msg = format_error_message(test_path, nospc_error)
        self.assertIn("No space left", msg)
        self.assertIn("Destination disk is full", msg)

        # Test generic IOError
        io_error = IOError("Disk read error")
        msg = format_error_message(test_path, io_error)
        self.assertIn("Disk read error", msg)
        self.assertIn("Check disk health", msg)

        # Test generic OSError
        os_error = OSError("Another OS error")
        msg = format_error_message(test_path, os_error)
        self.assertIn("Another OS error", msg)
        self.assertIn("Check disk health", msg)
        self.assertNotIn("Destination disk is full", msg)

        # Test generic Exception
        generic_error = Exception("Something else went wrong")
        msg = format_error_message(test_path, generic_error)
        self.assertIn("Something else went wrong", msg)
        self.assertNotIn("Suggestions:", msg)

    def test_clean_extensions(self):
        """Test the clean_extensions function."""
        # Test basic cleaning and normalization
        ext_list1 = [".jpg", "png", " .gif ", "mov", "."]
        cleaned1 = clean_extensions(ext_list1)
        self.assertEqual(cleaned1, ["*.jpg", "*.png", "*.gif", "*.mov", "*."])

        # Test with already wildcarded extensions
        ext_list2 = ["*.jpeg", "mp3"]
        cleaned2 = clean_extensions(ext_list2)
        self.assertEqual(cleaned2, ["*.jpeg", "*.mp3"])

        # Test with empty list
        self.assertEqual(clean_extensions([]), [])

        # Test with None
        self.assertEqual(clean_extensions(None), [])

        # Test with mixed cases and spaces
        ext_list3 = ["  .TXT", "JPEG  ", "PnG", ".mp3"]
        cleaned3 = clean_extensions(ext_list3)
        self.assertEqual(cleaned3, ["*.txt", "*.jpeg", "*.png", "*.mp3"])

        # Test with a single dot and other extensions
        ext_list4 = [".", "txt"]
        cleaned4 = clean_extensions(ext_list4)
        self.assertEqual(cleaned4, ["*.", "*.txt"])


if __name__ == "__main__":
    unittest.main()
