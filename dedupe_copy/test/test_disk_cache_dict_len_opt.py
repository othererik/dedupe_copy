import os
import unittest
from dedupe_copy import disk_cache_dict
from dedupe_copy.test import utils

class TestSqliteBackendLenOptimization(unittest.TestCase):
    """Test suite for verifying the optimized __len__ implementation in SqliteBackend."""

    def setUp(self):
        self.temp_dir = utils.make_temp_dir("dcd_len_test")
        self.db_file = os.path.join(self.temp_dir, "test_len.db")
        self.backend = disk_cache_dict.SqliteBackend(db_file=self.db_file)

    def tearDown(self):
        self.backend.close()
        utils.remove_dir(self.temp_dir)

    def test_len_correctness(self):
        """Verify __len__ returns correct values after various operations."""
        # 1. Initial state
        self.assertEqual(len(self.backend), 0, "Initial length should be 0")

        # 2. Insert items
        count = 100
        for i in range(count):
            self.backend[f"key{i}"] = i
        self.backend.commit()
        self.assertEqual(len(self.backend), count, f"Length should be {count} after insertion")

        # 3. Update existing items (length should not change)
        for i in range(10):
            self.backend[f"key{i}"] = i + 1000
        self.backend.commit()
        self.assertEqual(len(self.backend), count, "Length should not change after updating existing keys")

        # 4. Insert new items via update_batch
        new_items = {f"new{i}": i for i in range(50)}
        self.backend.update_batch(new_items)
        count += 50
        self.assertEqual(len(self.backend), count, f"Length should be {count} after update_batch")

        # 5. Delete items
        for i in range(10):
            del self.backend[f"key{i}"]
        self.backend.commit()
        count -= 10
        self.assertEqual(len(self.backend), count, f"Length should be {count} after deletion")

        # 6. Clear
        self.backend.clear()
        self.assertEqual(len(self.backend), 0, "Length should be 0 after clear")

    def test_len_persistence(self):
        """Verify __len__ is correct after closing and reopening the database."""
        count = 50
        for i in range(count):
            self.backend[f"key{i}"] = i
        self.backend.commit()
        self.assertEqual(len(self.backend), count)

        # Close and reopen
        self.backend.close()

        new_backend = disk_cache_dict.SqliteBackend(db_file=self.db_file)
        try:
            self.assertEqual(len(new_backend), count, "Length should be persisted after reopen")

            # Add more items
            new_backend["extra"] = 999
            new_backend.commit()
            self.assertEqual(len(new_backend), count + 1)
        finally:
            new_backend.close()

    def test_len_load(self):
        """Verify __len__ is correct after load()."""
        count = 20
        for i in range(count):
            self.backend[f"key{i}"] = i
        self.backend.commit()

        # Create another DB file
        other_db_file = os.path.join(self.temp_dir, "other.db")
        other_backend = disk_cache_dict.SqliteBackend(db_file=other_db_file)
        try:
            other_backend["other"] = 1
            other_backend.commit()

            # Load the first DB into the second backend
            other_backend.load(self.db_file)
            self.assertEqual(len(other_backend), count, "Length should match loaded DB")
        finally:
            other_backend.close()

if __name__ == "__main__":
    unittest.main()
