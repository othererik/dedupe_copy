"""Performance tests for the Manifest class."""

import os
import time
import unittest
from collections import defaultdict

from dedupe_copy.manifest import Manifest
from dedupe_copy.test import utils


class TestManifestPerformance(unittest.TestCase):
    """Performance tests for the Manifest class."""

    def setUp(self):
        """Set up a temporary directory for test artifacts."""
        self.temp_dir = utils.make_temp_dir("manifest_perf")

    def tearDown(self):
        """Clean up the temporary directory."""
        utils.remove_dir(self.temp_dir)

    def test_remove_files_performance(self):
        """Test the performance of the remove_files method with a large manifest."""
        manifest_path = os.path.join(self.temp_dir, "large_manifest.db")
        manifest = Manifest(
            manifest_paths=None, save_path=manifest_path, temp_directory=self.temp_dir
        )

        # Populate the manifest with a large number of files
        num_files = 50000
        num_hashes = 10000  # Use more hashes to keep file lists per hash smaller
        files_to_remove = []

        # Use a temporary dict to build the data efficiently in memory first
        temp_md5_data = defaultdict(list)
        temp_read_sources = {}

        print("Populating test data in memory...")
        for i in range(num_files):
            hash_val = f"hash_{(i % num_hashes)}"
            file_path = f"/test/file_{i}.txt"
            file_info = (file_path, 1024, time.time())

            temp_md5_data[hash_val].append(file_info)
            temp_read_sources[file_path] = None

            if i % 10 == 0:
                files_to_remove.append(file_path)

        print("Batch writing test data to manifest DB...")
        # Batch write to the DefaultCacheDict to avoid slow read-modify-write cycles
        for key, value in temp_md5_data.items():
            manifest.md5_data[key] = value
        for key, value in temp_read_sources.items():
            manifest.read_sources[key] = value
        manifest.md5_data.save()
        manifest.read_sources.save()
        print("Test data populated.")

        # Time the remove_files operation
        start_time = time.time()
        manifest.remove_files(files_to_remove)
        end_time = time.time()

        duration = end_time - start_time
        print(
            f"remove_files duration for {len(files_to_remove)} files "
            f"from {num_files}: {duration:.4f} seconds"
        )

        # Verify that the correct number of files were removed
        expected_remaining = num_files - len(files_to_remove)
        self.assertEqual(len(manifest.read_sources), expected_remaining)

        # Also verify the contents of md5_data for correctness
        files_to_remove_set = set(files_to_remove)
        remaining_files_count = 0
        for _, file_list in manifest.md5_data.items():
            for file_info in file_list:
                self.assertNotIn(file_info[0], files_to_remove_set)
                remaining_files_count += 1
        self.assertEqual(remaining_files_count, expected_remaining)

        # Assert that the operation completes within a reasonable time.
        # With the fix, this should be very fast. The original implementation would time out.
        self.assertLess(duration, 10.0, "remove_files took too long to execute.")

        manifest.close()


if __name__ == "__main__":
    unittest.main()
