"""Test for lazy iteration of SqliteBackend."""

import os
import unittest
from unittest import mock

from dedupe_copy import disk_cache_dict
from dedupe_copy.test import utils


# pylint: disable=protected-access


class TestSqliteBackendLazy(unittest.TestCase):
    """Test suite for lazy iteration of SqliteBackend."""

    def setUp(self):
        self.temp_dir = utils.make_temp_dir("dcd_lazy_test")
        self.db_file = os.path.join(self.temp_dir, "test_lazy.db")
        self.backend = disk_cache_dict.SqliteBackend(db_file=self.db_file)

    def tearDown(self):
        if self.backend:
            self.backend.close()
        utils.remove_dir(self.temp_dir)

    def test_iter_is_lazy(self):
        """Verify that __iter__ does not load all items immediately."""
        # Add some items
        for i in range(10):
            self.backend[f"key_{i}"] = f"value_{i}"
        self.backend.commit()

        # Mock _load to track calls
        # Note: _load is a static method, so we patch it on the class or instance.
        # Since we are calling self.backend._load, mocking on instance or class works.
        # However, because it's static, mocking on the class is safer.
        original_load = disk_cache_dict.SqliteBackend._load

        with mock.patch.object(
            disk_cache_dict.SqliteBackend, "_load", side_effect=original_load
        ) as mock_load:
            # Create iterator
            iterator = iter(self.backend)

            # In a lazy implementation (generator), creating the iterator
            # should NOT trigger any loads yet (until next() is called).
            # In an eager implementation (list), creating the iterator
            # would trigger loads for all items.

            # Verify no calls yet
            self.assertEqual(
                mock_load.call_count,
                0,
                "Iterator should be lazy and not call _load immediately upon creation",
            )

            # Consume one item
            next(iterator)

            # verify that _load was called (at least once for the key)
            self.assertGreaterEqual(mock_load.call_count, 1)


if __name__ == "__main__":
    unittest.main()
