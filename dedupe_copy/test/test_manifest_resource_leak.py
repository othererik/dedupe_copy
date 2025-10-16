import os
import sqlite3
import tempfile
import unittest
from dedupe_copy.manifest import Manifest

class TestManifestResourceLeak(unittest.TestCase):
    def test_manifest_list_resource_leak(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_paths = []
            # The number of manifests to create. This should be high enough
            # to exceed the per-process limit on open file descriptors.
            # On many systems, the soft limit is 1024, so we create more.
            num_manifests = 1100
            for i in range(num_manifests):
                # Create a dummy manifest file
                manifest_path = os.path.join(temp_dir, f"manifest_{i}.db")
                m = Manifest(manifest_paths=None, save_path=manifest_path)
                m.md5_data[f"hash_{i}"] = [(f"file_{i}", 123, 456.789)]
                m.save()
                m.close()
                manifest_paths.append(manifest_path)

            # This should now pass without raising an exception
            combined_manifest = Manifest(manifest_paths=manifest_paths, temp_directory=temp_dir)
            self.assertGreater(len(combined_manifest), 0)
            combined_manifest.close()

if __name__ == '__main__':
    unittest.main()
