"""
Tests for dedupe_copy.disk_cache_dict coverage.
"""

import unittest
import sqlite3
import pickle
from unittest.mock import MagicMock
from dedupe_copy.disk_cache_dict import (
    _deserialize,
    SqliteBackend,
    SqliteSetBackend,
    DefaultCacheDict,
)


class TestDiskCacheDictCoverage(unittest.TestCase):
    """
    Tests specifically designed to cover edge cases and exception handling
    in dedupe_copy.disk_cache_dict to improve test coverage.
    """

    # pylint: disable=protected-access

    def test_deserialize_edge_cases(self):
        """Test _deserialize with various edge case inputs."""
        # Test empty bytes
        self.assertIsNone(_deserialize(b""))

        # Test None type marker (b'N')
        self.assertIsNone(_deserialize(b"N"))

        # Test boolean True (b'X' is used for bool in some versions/implementations,
        # though _serialize uses 'B'. The code supports 'X' as True)
        self.assertTrue(_deserialize(b"X1"))
        self.assertFalse(_deserialize(b"X0"))

        # Test explicit pickle marker (b'P')
        data = {"key": "value"}
        pickled = b"P" + pickle.dumps(data)
        self.assertEqual(_deserialize(pickled), data)

        # Test legacy pickle (no marker, fallback)
        # We need a byte string that doesn't start with known markers (S, I, B, F, X, N, P)
        # Pickle protocol 4+ usually starts with \x80
        legacy_pickled = pickle.dumps(data)
        self.assertEqual(_deserialize(legacy_pickled), data)

    def test_sqlite_backend_update_batch_exception(self):
        """Test exception handling in SqliteBackend.update_batch."""
        backend = SqliteBackend(db_file=":memory:")

        # Mock the connection to raise an error on executemany
        original_conn = backend.conn
        mock_conn = MagicMock(wraps=original_conn)
        mock_conn.executemany.side_effect = sqlite3.Error("Simulated error")

        # We need to inject our mock connection.
        # Since backend.conn is a property that calls _init_conn if _conn is None,
        # we can just set _conn directly.
        backend._conn = mock_conn

        with self.assertRaises(sqlite3.Error):
            backend.update_batch({"key": "value"})

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()

        backend.close()

    def test_sqlite_backend_commit_batch_exception(self):
        """Test exception handling in SqliteBackend._commit_batch."""
        backend = SqliteBackend(db_file=":memory:")

        # Add simpler item to trigger _write_batch population
        # We manually access _write_batch to avoid triggering commit during __setitem__
        backend._write_batch["key"] = "value"

        original_conn = backend.conn
        mock_conn = MagicMock(wraps=original_conn)
        mock_conn.executemany.side_effect = sqlite3.Error("Simulated commit error")
        backend._conn = mock_conn

        with self.assertRaises(sqlite3.Error):
            backend._commit_batch()

        mock_conn.rollback.assert_called_once()

        # Clear side effect to allow clean close
        mock_conn.executemany.side_effect = None
        backend.close()

    def test_sqlite_set_backend_update_batch_exception(self):
        """Test exception handling in SqliteSetBackend.update_batch."""
        backend = SqliteSetBackend(db_file=":memory:")

        original_conn = backend.conn
        mock_conn = MagicMock(wraps=original_conn)
        mock_conn.executemany.side_effect = sqlite3.Error("Simulated set error")
        backend._conn = mock_conn

        with self.assertRaises(sqlite3.Error):
            backend.update_batch(["item1", "item2"])

        mock_conn.rollback.assert_called_once()

        # Clear side effect to allow clean close
        mock_conn.executemany.side_effect = None
        backend.close()

    def test_sqlite_set_backend_commit_batch_exception(self):
        """Test exception handling in SqliteSetBackend._commit_batch."""
        backend = SqliteSetBackend(db_file=":memory:")

        # Manually populate write batch
        backend._write_batch.add("item1")

        original_conn = backend.conn
        mock_conn = MagicMock(wraps=original_conn)
        mock_conn.executemany.side_effect = sqlite3.Error("Simulated set commit error")
        backend._conn = mock_conn

        with self.assertRaises(sqlite3.Error):
            backend._commit_batch()

        mock_conn.rollback.assert_called_once()

        # Clear side effect to allow clean close
        mock_conn.executemany.side_effect = None
        backend.close()

    def test_default_cache_dict_flush_race_condition(self):
        """
        Test _flush_batch_to_db handling when keys are missing from cache.
        This simulates a race condition where the cache might be cleared
        or items removed by another thread before flush.
        """
        d = DefaultCacheDict(max_size=10, db_file=":memory:")
        d["a"] = 1
        d["b"] = 2

        # Manually trigger flush with data that includes keys NOT in cache
        # This hits the 'else' branch in _flush_batch_to_db (len(data) <= len(self._cache))
        # or just general key missing handling if we manipulate it right.

        # Case 1: Keys in data but not in cache
        data = {"c": 3}
        # 'c' is not in d._cache, so it should just be written to DB
        # and not raise KeyError during cache deletion attempt
        d._flush_batch_to_db(data)
        self.assertEqual(d["c"], 3)  # Should be in DB now

        # Case 2: Keys in data AND in cache (normal case)
        data = {"a": 10}
        d._flush_batch_to_db(data)
        self.assertNotIn("a", d._cache)  # Should be evicted from cache
        self.assertEqual(d["a"], 10)  # Should be in DB

    def test_default_cache_dict_clear_fallback(self):
        """Test DefaultCacheDict.clear() when backend doesn't support clear()."""

        # pylint: disable=protected-access
        # Create a dummy backend without a clear method
        class DummyBackend(dict):
            """Dummy backend for testing."""

            def __init__(self):
                super().__init__()
                self.cleared = False

            # No clear() method

        backend = DummyBackend()
        backend["a"] = 1
        backend["b"] = 2

        d = DefaultCacheDict(db_file=":memory:", backend=backend)
        d["c"] = 3  # In cache

        d.clear()

        self.assertEqual(len(d), 0)
        self.assertEqual(len(backend), 0)
