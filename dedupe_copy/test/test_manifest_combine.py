"""Tests for combining manifests"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from dedupe_copy.disk_cache_dict import DefaultCacheDict, CacheDict
from dedupe_copy.manifest import Manifest


class TestManifestCombine(unittest.TestCase):
    """Tests for combining manifests"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

    def test_combine_manifests_closes_handles(self):
        """Verify that file handles are closed during manifest combination"""
        manifests_to_combine = []
        for i in range(2):
            # Create a mock manifest (tuple of md5_data and read_sources)
            md5_data = DefaultCacheDict(list, db_file=os.path.join(self.temp_dir.name, f"md5_{i}.db"))
            md5_data[f"hash_{i}"] = [[f"/path/to/file_{i}", 100, 12345.6]]
            # Replace the close method with a mock to track calls
            md5_data.close = MagicMock()

            read_sources = CacheDict(db_file=os.path.join(self.temp_dir.name, f"read_{i}.db"))
            read_sources[f"/path/to/file_{i}"] = None
            # Replace the close method with a mock to track calls
            read_sources.close = MagicMock()

            manifests_to_combine.append((md5_data, read_sources))

        # Call the method under test
        combined_md5, combined_read = Manifest._combine_manifests(
            manifests_to_combine, self.temp_dir.name
        )

        # Clean up the combined manifests to prevent file lock issues
        combined_md5.close()
        combined_read.close()

        # Check that the close method was called on each original manifest part
        for md5_data, read_sources in manifests_to_combine:
            md5_data.close.assert_called_once()
            read_sources.close.assert_called_once()