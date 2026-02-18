"""
Tests for dedupe_copy.utils coverage.
"""

import unittest
import hashlib
import tempfile
import os
import shutil
from unittest.mock import patch
from dedupe_copy.utils import (
    hash_file,
    match_extension,
    ExtensionMatcher,
    clean_extensions,
)


class TestUtilsCoverage(unittest.TestCase):
    """
    Tests specifically designed to cover edge cases and fallback logic
    in dedupe_copy.utils to improve test coverage.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test_file.txt")
        with open(self.test_file, "wb") as f:
            f.write(b"test content")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_hash_file_fallback_manual_chunking(self):
        """
        Test hash_file fallback logic when hashlib.file_digest is NOT available.
        This forces the code to use the manual read-chunk-update loop.
        """
        # Create a mock hashlib module that lacks file_digest

        # We need to patch hashlib in the utils module namespace,
        # or globally if it's imported directly
        # based on how it's used in utils.py: "if hasattr(hashlib, 'file_digest'):"

        with patch("dedupe_copy.utils.hashlib") as mock_hashlib:
            # Configure mock to behave like standard hashlib but without file_digest
            del mock_hashlib.file_digest

            # We need to pass through legitimate calls like md5()
            mock_hashlib.md5.side_effect = hashlib.md5

            # Calculate expected hash using standard method
            expected_hash = hashlib.md5(b"test content").hexdigest()

            # Logic under test
            result_hash = hash_file(self.test_file, hash_algo="md5")

            self.assertEqual(result_hash, expected_hash)

    def test_match_extension_polymorphism(self):
        """
        Test match_extension with an ExtensionMatcher instance.
        This covers the isinstance check in match_extension.
        """
        matcher = ExtensionMatcher(["txt"])

        # Should match
        self.assertTrue(match_extension(matcher, "file.txt"))

        # Should not match
        self.assertFalse(match_extension(matcher, "file.jpg"))

    def test_clean_extensions_edge_cases(self):
        """
        Test clean_extensions with edge cases like wildcards combined with dots.
        """
        # Case from line 271: ext.startswith(".") and contains wildcard
        # e.g., ".[a-z]" -> "*.[a-z]"

        inputs = [".[a-z]"]
        # The code logic:
        # if startswith("."):
        #   if wildcards:
        #      append(f"*{ext}") -> "*.[a-z]"

        cleaned = clean_extensions(inputs)
        self.assertEqual(cleaned, ["*.[a-z]"])

        # Verify it works with match_extension (integration check)
        # "*.[a-z]" should match "file.c"
        self.assertTrue(match_extension(cleaned, "file.c"))
        self.assertFalse(
            match_extension(cleaned, "file.txt")
        )  # "txt" is 3 chars, [a-z] is 1 char
