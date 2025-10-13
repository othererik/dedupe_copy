"""Tests for hashing functions."""

import os
import time
import unittest
from unittest import mock
from dedupe_copy.utils import hash_file


class TestHashing(unittest.TestCase):
    """Quick test to verify hashing functions work as expected."""

    def setUp(self):
        self.test_file = f"test_file{time.time()}.txt"
        with open(self.test_file, "w", encoding="utf-8") as f:
            f.write("This is a test file for hashing.")

    def tearDown(self):
        os.remove(self.test_file)

    def test_md5_hashing(self):
        """Verify MD5 hashing produces the correct hash."""
        expected_md5 = "c0bebe9ec49ac60275d09d2187fc2235"
        actual_md5 = hash_file(self.test_file, hash_algo="md5")
        self.assertEqual(actual_md5, expected_md5)

    def test_xxhash_hashing(self):
        """Verify xxHash hashing produces the correct hash."""
        if hash_file.__globals__.get("xxhash") is None:
            self.skipTest("xxhash module not available")
        expected_xxhash = "8471ab391af8733a"
        actual_xxhash = hash_file(self.test_file, hash_algo="xxh64")
        self.assertEqual(actual_xxhash, expected_xxhash)

    def test_xxhash_hashing_fails_if_not_installed(self):
        """Verify hash_file raises RuntimeError if xxh64 is requested but not installed."""
        # Patch 'xxhash' in the utils module to None to simulate it not being installed
        with mock.patch("dedupe_copy.utils.xxhash", None):
            with self.assertRaises(RuntimeError) as cm:
                hash_file(self.test_file, hash_algo="xxh64")
            self.assertIn("the 'xxhash' library is not installed", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
