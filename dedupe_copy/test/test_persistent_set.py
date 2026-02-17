"""Test suite for PersistentSet."""

import unittest
import os
import sqlite3
import random
from dedupe_copy import disk_cache_dict
from dedupe_copy.test import utils


class TestPersistentSet(unittest.TestCase):
    """Test suite for PersistentSet."""

    def setUp(self):
        self.temp_dir = utils.make_temp_dir("set_temp")
        self.db_file = os.path.join(
            self.temp_dir, f"test_set_{random.getrandbits(16)}.db"
        )
        self.pset = disk_cache_dict.PersistentSet(max_size=10, db_file=self.db_file)

    def tearDown(self):
        self.pset.close()
        utils.remove_dir(self.temp_dir)

    def test_add_contains(self):
        """Test adding items and checking existence."""
        self.pset.add("a")
        self.assertIn("a", self.pset)
        self.assertNotIn("b", self.pset)

        # Add more items to trigger eviction
        for i in range(20):
            self.pset.add(f"key_{i}")

        for i in range(20):
            self.assertIn(f"key_{i}", self.pset)

        self.assertIn("a", self.pset)

    def test_len(self):
        """Test length calculation."""
        for i in range(15):
            self.pset.add(f"key_{i}")

        self.assertEqual(len(self.pset), 15)

        # Add duplicate
        self.pset.add("key_0")
        self.assertEqual(len(self.pset), 15)

    def test_discard(self):
        """Test removing items."""
        self.pset.add("a")
        self.pset.add("b")
        self.pset.discard("a")
        self.assertNotIn("a", self.pset)
        self.assertIn("b", self.pset)

        # Discard non-existent
        self.pset.discard("c")  # Should not raise error

    def test_iter(self):
        """Test iteration."""
        items = {f"key_{i}" for i in range(15)}
        for item in items:
            self.pset.add(item)

        iterated = set(self.pset)
        self.assertEqual(items, iterated)

    def test_clear(self):
        """Test clearing the set."""
        for i in range(15):
            self.pset.add(f"key_{i}")
        self.pset.clear()
        self.assertEqual(len(self.pset), 0)
        self.assertNotIn("key_0", self.pset)

    def test_save_load(self):
        """Test save and load."""
        for i in range(15):
            self.pset.add(f"key_{i}")
        self.pset.save()

        # Re-open
        self.pset.close()
        new_pset = disk_cache_dict.PersistentSet(db_file=self.db_file)
        new_pset.load()

        self.assertEqual(len(new_pset), 15)
        for i in range(15):
            self.assertIn(f"key_{i}", new_pset)
        new_pset.close()

    def test_update(self):
        """Test batch update."""
        items = [f"key_{i}" for i in range(20)]
        self.pset.update(items)
        self.assertEqual(len(self.pset), 20)
        for item in items:
            self.assertIn(item, self.pset)

    def test_migration(self):
        """Test migration from SqliteBackend (CacheDict) table."""
        legacy_db = os.path.join(self.temp_dir, "legacy.db")

        # Create legacy DB using SqliteBackend
        backend = disk_cache_dict.SqliteBackend(db_file=legacy_db)
        backend["old_key_1"] = None
        backend["old_key_2"] = "some value"  # Value is ignored in set
        backend.commit()
        backend.close()

        # Open with SqliteSetBackend (via PersistentSet)
        pset = disk_cache_dict.PersistentSet(db_file=legacy_db)

        # Check if migrated
        self.assertIn("old_key_1", pset)
        self.assertIn("old_key_2", pset)
        self.assertEqual(len(pset), 2)

        # Check if table was migrated
        # We can inspect the DB
        conn = sqlite3.connect(legacy_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sql_dict_table';"
        )
        self.assertIsNone(cursor.fetchone(), "Legacy table should be dropped")

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sql_set_table';"
        )
        self.assertIsNotNone(cursor.fetchone(), "New table should exist")
        conn.close()
        pset.close()

if __name__ == "__main__":
    unittest.main()
