"""Tests for manifest memory usage."""

import unittest
import os
import tempfile
import shutil
from dedupe_copy.manifest import Manifest


class TestManifestMemory(unittest.TestCase):
    """Test cases for manifest memory efficiency."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.manifest_path = os.path.join(self.temp_dir, "test_manifest.db")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_populate_read_sources_memory_efficient(self):
        """Verify that _populate_read_sources works correctly without loading
        everything into memory."""
        manifest = Manifest(
            None, save_path=self.manifest_path, temp_directory=self.temp_dir
        )

        # Mock md5_data to simulate a large number of files
        # We use a generator or a custom iterator to avoid creating the dict in
        # memory for the test setup if possible, but for this test, we just want
        # to ensure the logic works.
        # Real memory usage testing is hard in a unit test without psutil,
        # so we verify correctness of the new logic.

        # Create a lot of dummy data
        num_files = 1000
        dummy_data = {}
        for i in range(num_files):
            key = f"hash_{i}"
            # path, size, mtime
            dummy_data[key] = [[f"/path/to/file_{i}", 100, 1234567890.0]]

        manifest.md5_data = dummy_data

        # Clear read_sources to ensure it gets populated
        manifest.read_sources.clear()

        # pylint: disable=protected-access
        manifest._populate_read_sources()

        # Verify results
        self.assertEqual(len(manifest.read_sources), num_files)
        for i in range(num_files):
            self.assertIn(f"/path/to/file_{i}", manifest.read_sources)


if __name__ == "__main__":
    unittest.main()
