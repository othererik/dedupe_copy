import os
import shutil
import tempfile
import unittest
from collections import OrderedDict

from dedupe_copy.disk_cache_dict import CacheDict, SqliteBackend

class TestCacheBatch(unittest.TestCase):
    """Test suite for CacheDict batch update functionality."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_file = os.path.join(self.temp_dir, "test.db")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_update_batch_fits_in_cache(self):
        """Test batch update when data fits in the cache."""
        cd = CacheDict(db_file=self.db_file, max_size=100)
        data = {f"k{i}": f"v{i}" for i in range(10)}

        cd.update_batch(data)

        # Verify items are in cache and not DB (yet)
        self.assertEqual(len(cd._cache), 10)
        # Note: cd._db items are not populated until eviction or save,
        # but update_batch logic keeps them in cache if they fit.
        # Actually, if they are in cache, they are NOT in DB.

        # Verify accessibility
        for i in range(10):
            self.assertEqual(cd[f"k{i}"], f"v{i}")

        cd.close()

    def test_update_batch_exceeds_cache(self):
        """Test batch update when data exceeds cache size."""
        cd = CacheDict(db_file=self.db_file, max_size=5)
        data = {f"k{i}": f"v{i}" for i in range(10)}

        # This should trigger the DB update path
        cd.update_batch(data)

        # Verify items are accessible (faulted in from DB)
        for i in range(10):
            self.assertEqual(cd[f"k{i}"], f"v{i}")

        # Verify size
        self.assertEqual(len(cd), 10)

        cd.close()

    def test_update_batch_mixed(self):
        """Test updating existing items in cache via batch."""
        cd = CacheDict(db_file=self.db_file, max_size=100)

        # Initial population
        cd["k1"] = "old_v1"
        cd["k2"] = "v2"

        # Batch update that updates k1 and adds k3
        update_data = {"k1": "new_v1", "k3": "v3"}
        cd.update_batch(update_data)

        self.assertEqual(cd["k1"], "new_v1")
        self.assertEqual(cd["k2"], "v2")
        self.assertEqual(cd["k3"], "v3")

        cd.close()

    def test_update_batch_mixed_exceeds(self):
        """Test updating existing items when batch exceeds cache."""
        # Setup: cache size 5. Populate with 3 items.
        cd = CacheDict(db_file=self.db_file, max_size=5)
        cd["old1"] = "v1"
        cd["old2"] = "v2"
        cd["old3"] = "v3"

        # Update batch with 10 items, including updates to old items
        data = {f"new{i}": f"val{i}" for i in range(8)}
        data["old1"] = "updated_v1"
        data["old2"] = "updated_v2"

        # This should force DB update path because 10 > (5 - 3)
        cd.update_batch(data)

        # Verify updates
        self.assertEqual(cd["old1"], "updated_v1")
        self.assertEqual(cd["old2"], "updated_v2")
        self.assertEqual(cd["old3"], "v3")
        self.assertEqual(cd["new0"], "val0")

        # Verify total count
        # 3 initial - 0 removed (updated ones are removed and re-added effectively)
        # old1 and old2 were in cache. update_batch logic removes them from cache and updates DB.
        # old3 remains in cache.
        # So we have old3 in cache, and everything else in DB (until accessed).
        self.assertEqual(len(cd), 3 + 8) # 11 items

        cd.close()

    def test_init_with_batch(self):
        """Test initialization with a dictionary (uses update_batch)."""
        data = {f"k{i}": f"v{i}" for i in range(10)}
        cd = CacheDict(db_file=self.db_file, current_dictionary=data, max_size=100)

        self.assertEqual(len(cd), 10)
        self.assertEqual(cd["k0"], "v0")

        cd.close()

    def test_lru_behavior_batch_fits(self):
        """Test LRU order is updated when batch fits in cache."""
        cd = CacheDict(db_file=self.db_file, max_size=10, lru=True)

        cd["k1"] = "v1"
        cd["k2"] = "v2"
        # Order: k1, k2

        cd.update_batch({"k1": "v1_new", "k3": "v3"})
        # Expected Order: k2, k1, k3

        keys = list(cd._key_order.keys())
        self.assertEqual(keys, ["k2", "k1", "k3"])

        cd.close()

    def test_lru_behavior_batch_exceeds(self):
        """Test LRU order when batch exceeds cache."""
        cd = CacheDict(db_file=self.db_file, max_size=5, lru=True)

        cd["k1"] = "v1"
        cd["k2"] = "v2"

        # Batch update exceeds max_size (10 items)
        data = {f"new{i}": f"v{i}" for i in range(10)}
        data["k1"] = "v1_new"

        # This triggers DB update. k1 is removed from cache/LRU.
        cd.update_batch(data)

        # Verify k1 is removed from LRU
        self.assertNotIn("k1", cd._key_order)
        # k2 should still be there
        self.assertIn("k2", cd._key_order)

        # Verify k1 value is updated (via DB)
        self.assertEqual(cd["k1"], "v1_new")
        # Accessing k1 brings it back to cache/LRU
        self.assertIn("k1", cd._key_order)

        cd.close()

    def test_empty_batch(self):
        cd = CacheDict(db_file=self.db_file)
        cd.update_batch({})
        self.assertEqual(len(cd), 0)
        cd.close()
