"""Tests around loading and saving manifests"""

import os
import unittest
from unittest.mock import patch

from dedupe_copy.test import utils

from dedupe_copy.manifest import Manifest
from dedupe_copy.disk_cache_dict import DefaultCacheDict, CacheDict


class TestManifests(unittest.TestCase):
    """Test load/save of manifests individually and in a group. These are
    happy path tests.
    """

    def setUp(self):
        """Create temporary directory and test data"""
        self.temp_dir = utils.make_temp_dir("manifests")
        self.temp_dict = os.path.join(self.temp_dir, "tempdict.dict")
        self.scratch_dict = os.path.join(self.temp_dir, "scratch.dict")
        self.manifest_path = os.path.join(self.temp_dir, "manifest.dict")
        self.read_path = f"{self.manifest_path}.read"
        self.manifest = Manifest(None, save_path=None, temp_directory=self.temp_dir)

    def tearDown(self):
        """Remove temporary directory and all test files"""
        del self.manifest
        utils.remove_dir(self.temp_dir)

    def setup_manifest(self):
        """Set up a test manifest with fake data."""
        md5data, sources = utils.gen_fake_manifest()
        md5data = _dcd_from_manifest(md5data, self.temp_dict)
        sources = _dcd_from_manifest(sources, self.scratch_dict)
        self.manifest.md5_data = md5data
        self.manifest.read_sources = sources
        return md5data, sources

    def check_manifest(self, manifest, md5_data, sources):
        """Verify manifest contents match expected data."""
        print(manifest.md5_data.items())
        print(manifest.read_sources.items())
        print(md5_data.items())
        print(sources.items())
        self.assertEqual(
            sorted(sources.keys()),
            sorted(manifest.read_sources.keys()),
            "sources does not agree with manifest sources",
        )
        self.assertEqual(
            sorted(md5_data.keys()),
            sorted(manifest.md5_data.keys()),
            "data keys does not agree",
        )
        for key, values in md5_data.items():
            for index, meta_data in enumerate(values):
                self.assertEqual(
                    meta_data,
                    manifest.md5_data[key][index],
                    f"Meta data mismatch for key {key}",
                )
        del md5_data
        del sources

    def test_save(self):
        """Create a manifest, save it and directly get a DCD to test"""
        md5data, sources = self.setup_manifest()
        self.manifest.save(path=self.manifest_path)
        dcd_check = DefaultCacheDict(list, db_file=self.manifest_path)
        sources_check = DefaultCacheDict(list, db_file=self.read_path)
        self.check_manifest(self.manifest, md5data, sources)
        self.check_manifest(self.manifest, dcd_check, sources_check)
        del md5data
        del sources

    def test_manifest_conversion(self):
        """Manifest path translation"""
        md5data, sources = self.setup_manifest()
        self.manifest.save(path=self.manifest_path)
        self.manifest.convert_manifest_paths(
            "/a/b", "/fred", temp_directory=self.temp_dir
        )
        for path in self.manifest.read_sources.keys():
            self.assertTrue(
                (not path.startswith("/a/b"))
                and (path.startswith("/c/b") or path.startswith("/fred"))
            )
        for metadata in self.manifest.md5_data.values():
            for items in metadata:
                path = items[0]
                self.assertTrue(
                    (not path.startswith("/a/b"))
                    and (path.startswith("/c/b") or path.startswith("/fred"))
                )
        del md5data
        del sources

    def test_load_single(self):
        """Load a previously saved manifest"""
        md5data, sources = utils.gen_fake_manifest()
        md5data = _dcd_from_manifest(md5data, self.temp_dict)
        sources = _dcd_from_manifest(sources, f"{self.temp_dict}.read")
        md5data.save()
        sources.save()
        manifest = Manifest(self.temp_dict, temp_directory=self.temp_dir)
        self.check_manifest(manifest, md5data, sources)
        del md5data
        del sources

    def test_load_list(self):
        """Loading a list of manifests"""
        master_md5 = {}
        master_sources = {}
        paths = []
        for i in range(5):
            md5data, sources = utils.gen_fake_manifest()
            path = f"{self.temp_dict}{i}"
            paths.append(path)
            md5data = _dcd_from_manifest(md5data, path)
            sources = _dcd_from_manifest(sources, f"{path}.read")
            master_md5.update(md5data)
            master_sources.update(sources)
            md5data.save()
            sources.save()
        combined = Manifest(paths, temp_directory=self.temp_dir)
        self.check_manifest(combined, master_md5, master_sources)
        del md5data
        del sources

    def test_avoids_double_close_on_combine(self):
        """Test that combining manifests does not double-close file handles."""
        paths = []
        for i in range(2):
            md5data, sources = utils.gen_fake_manifest()
            path = os.path.join(self.temp_dir, f"manifest_{i}.dict")
            paths.append(path)
            md5_dcd = _dcd_from_manifest(md5data, path)
            sources_dcd = _dcd_from_manifest(sources, f"{path}.read")
            md5_dcd.save()
            sources_dcd.save()
            md5_dcd.close()
            sources_dcd.close()

        with (
            patch.object(DefaultCacheDict, "close") as mock_dcd_close,
            patch.object(CacheDict, "close") as mock_cd_close,
        ):

            manifest = Manifest(paths, temp_directory=self.temp_dir)
            manifest.close()

            # Without the bug, the close method for each CacheDict type should
            # be called N times for the loaded manifests (in the finally block)
            # and once for the new combined manifest, totaling N+1 calls.
            # With 2 manifests, this means 3 calls each.
            # The bug causes an extra N calls, making it 2N+1, so 5 calls.
            self.assertEqual(mock_dcd_close.call_count, 3)
            self.assertEqual(mock_cd_close.call_count, 3)


def _dcd_from_manifest(data, path):
    dcd = DefaultCacheDict(list, db_file=path)
    dcd.update(data)
    return dcd
