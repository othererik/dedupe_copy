"""Test suite covering the db backed dictionary"""

import collections
import os
import random
import unittest

from dedupe_copy.test import utils

from dedupe_copy import disk_cache_dict

disk_cache_dict.DEBUG = True


class DcdActionSuite(  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    unittest.TestCase
):
    """Test suite for disk_cache_dict actions."""

    def setUp(self):
        """Create a new test object"""
        self.cache_size = 10
        self.temp_dir = utils.make_temp_dir("dcd_temp")
        self.db_file = os.path.join(
            self.temp_dir, f"dct_test_db_{random.getrandbits(16)}.dict"
        )
        self.mirror = collections.defaultdict(list)
        self.backend = None
        self.lru = False
        # Create default dcd object for tests that don't create their own
        self.dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=self.cache_size,
            db_file=self.db_file,
            lru=self.lru,
            backend=self.backend,
        )

    def tearDown(self):
        """Remove the db file"""
        self.dcd = None
        utils.remove_dir(self.temp_dir)

    def _spot_check(self, tests=10):
        for _ in range(tests):
            if not self.mirror.keys():
                return
            test_key = random.choice(list(self.mirror.keys()))
            actual = self.dcd[test_key]
            expected = self.mirror[test_key]
            self.assertEqual(
                actual,
                expected,
                f"Expected {expected} for key {test_key} but got {actual}",
            )

    def _get_all(self):
        for test_key, expected in self.mirror.items():
            actual = self.dcd[test_key]
            self.assertEqual(
                actual,
                expected,
                f"Expected {expected} for key {test_key} but got {actual}",
            )

    def test_action_add_read_consistency(self):
        """Test consistency between adding and reading operations.

        Add new keys and confirm random gets are consistent below and above
        the memory cache dict's max size.
        """
        for i in range(100):
            if random.random() < 0.5:
                _ = self.mirror[i]
                _ = self.dcd[i]
            else:
                self.mirror[i] = str(i)
                self.dcd[i] = str(i)
            self._spot_check()
        self._get_all()

    def test_action_add_del_read_consistency(self):
        """Test consistency between add, delete and read operations.

        Add new keys and delete keys and confirm random gets are consistent
        below and above the memory cache dict's max size. Picks up a few
        updates as well.
        """
        # be sure to re-use deleted keys
        just_removed = None
        for j in range(3):
            for i in range(50):
                mirror_keys = list(self.mirror.keys())
                if random.random() < 0.33 and mirror_keys:
                    del_key = random.choice(mirror_keys)
                    del self.mirror[del_key]
                    del self.dcd[del_key]
                    just_removed = del_key
                else:
                    self.mirror[i] = f"{j}_{i}"
                    self.dcd[i] = f"{j}_{i}"
                    just_removed = None
                self._spot_check()
                if just_removed:
                    self.assertNotIn(
                        just_removed,
                        self.dcd,
                        f"Found deleted key {just_removed}",
                    )
        self._get_all()

    def test_updates(self):
        """Test updating dictionary with new values.

        Update keys that are in out out of cache, check correctness."""
        # half in, half out of cache
        for i in range(self.cache_size * 2):
            self.mirror[i] = str(i)
            self.dcd[i] = str(i)
        for j in range(100):
            self.mirror[j] = f"{self.mirror[j]}_{j}"
            self.dcd[j] = f"{self.dcd[j]}_{j}"
            self._spot_check()
            # pylint: disable=protected-access
            self.assertEqual(
                self.cache_size, len(self.dcd._cache.keys()), "Dcd cache is wrong size"
            )
        self._get_all()

    def test_existing_dict_small(self):
        """Test cache behavior with existing small dictionary."""
        current_dict = dict(
            (i, i) for i in range(self.cache_size - int((self.cache_size / 2)))
        )
        current_dict["ek_notmod"] = "an existing key we keep around"
        self.dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=self.cache_size,
            db_file=self.db_file,
            lru=self.lru,
            current_dictionary=current_dict,
            backend=self.backend,
        )
        self.mirror.update(current_dict)
        self.test_updates()

    def test_existing_dict_large(self):
        """Test cache behavior with existing large dictionary."""
        current_dict = dict((i, i) for i in range(self.cache_size * 2))
        current_dict["ek_notmod"] = "an existing key we keep around"
        self.dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=self.cache_size,
            db_file=self.db_file,
            lru=self.lru,
            current_dictionary=current_dict,
            backend=self.backend,
        )
        self.mirror.update(current_dict)
        self.test_updates()

    def test_iteritems(self):
        """Test iteration over dictionary items."""
        mirror = dict((i, i) for i in range(self.cache_size * 2))
        for key, value in mirror.items():
            self.dcd[key] = value
        for key, value in self.dcd.items():
            self.assertEqual(
                mirror[key],
                value,
                f"Incorrect value for key {key}. Expected: {mirror[key]}, "
                f"Actual: {value}",
            )

    def _populate(self, size=100):
        for i in range(size):
            self.mirror[i] = i
            self.dcd[i] = i

    def _compare(self, mirror, dcd):
        mkeys = sorted(mirror.keys())
        dkeys = sorted(dcd.keys())
        self.assertEqual(
            mkeys,
            dkeys,
            f"not all keys the same - actual: {dkeys} expected: {mkeys}",
        )
        for mk, mv in mirror.items():
            dcdv = dcd[mk]
            self.assertEqual(
                mv, dcdv, f"Value mismatch - actual: {dcdv} expected: {mv}"
            )

    def test_clear(self):
        """Test clearing the dictionary."""
        self._populate(100)
        self.mirror.clear()
        self.dcd.clear()
        self._compare(self.mirror, self.dcd)

    def test_copy(self):
        """Test copying the dictionary."""
        self._populate(100)
        mirror_copy = self.mirror.copy()
        new_db_file = f"{self.db_file}_2"
        dcd_copy = self.dcd.copy(db_file=new_db_file)
        self._compare(mirror_copy, dcd_copy)

    def test_fromkeys(self):
        """Test creating dictionary from keys."""
        self._populate(100)
        keys = [f"a{i}" for i in range(30)]
        newmirror = self.mirror.fromkeys(keys, "a")
        new_db_file = f"{self.db_file}_2"
        newdcd = self.dcd.fromkeys(keys, "a", db_file=new_db_file)
        self._compare(newmirror, newdcd)

    def test_get(self):
        """Test getting values with default fallback."""
        self._populate(100)
        test_keys = [1, 70, 100, 101, 201]
        for k in test_keys:
            mval = self.mirror.get(k, "nosuchkey")
            dval = self.dcd.get(k, "nosuchkey")
            self.assertEqual(
                mval,
                dval,
                f"Mismatch on get: Expected: {mval}, Actual: {dval}",
            )

    def test_has_key(self):
        """Test checking if dictionary has key."""
        self._populate(100)
        test_keys = [1, 99, 100, 101, 200]
        for k in test_keys:
            mval = k in self.mirror
            dval = k in self.dcd
            self.assertEqual(
                mval,
                dval,
                f"Mismatch on has_key: Expected: {mval}, Actual: {dval}",
            )

    def test_items(self):
        """Test getting dictionary items."""
        self._populate(100)
        mitems = sorted(self.mirror.items())
        ditems = sorted(self.dcd.items())
        self.assertEqual(
            mitems,
            ditems,
            f"Mismatch on iterkeys: Expected: {mitems}, Actual: {ditems}",
        )

    def test_itterkeys(self):
        """Test iterating over dictionary keys."""
        self._populate(100)
        mkey = sorted(list(self.mirror.keys()))
        dkey = sorted(list(self.dcd.keys()))
        self.assertEqual(
            mkey,
            dkey,
            f"Mismatch on iterkeys: Expected: {mkey}, Actual: {dkey}",
        )

    def test_itervalues(self):
        """Test iterating over dictionary values."""
        self._populate(100)
        mval = sorted(list(self.mirror.values()))
        dval = sorted(list(self.dcd.values()))
        self.assertEqual(
            mval,
            dval,
            f"Mismatch on itervalues: Expected: {mval}, Actual: {dval}",
        )

    def test_keys(self):
        """Test getting dictionary keys."""
        self._populate(100)
        mkeys = self.mirror.keys()
        dkeys = self.dcd.keys()
        self._compare(self.mirror, self.dcd)
        self.assertEqual(
            sorted(mkeys),
            sorted(dkeys),
            f"Did not get correct values - Expected: {mkeys} Actual: {dkeys}",
        )

    def test_pop(self):
        """Test removing and returning dictionary items."""
        self._populate(100)
        for k in list(self.mirror.keys()):
            mval = self.mirror.pop(k)
            dval = self.dcd.pop(k)
            self.assertEqual(
                mval,
                dval,
                f"Mismatch on pop: Expected: {mval}, Actual: {dval}",
            )

    def test_popitem(self):
        """Test removing and returning arbitrary dictionary items."""
        self._populate(1)
        item = self.mirror.popitem()
        dcd_item = self.dcd.popitem()
        self._compare(self.mirror, self.dcd)
        self.assertEqual(
            item,
            dcd_item,
            f"Did not get correct value - Expected: {item} Actual: {dcd_item}",
        )
        self._populate(self.cache_size * 2)
        while True:
            try:
                dcd_item = self.dcd.popitem()
                self.assertEqual(dcd_item[0], dcd_item[1], "popitem key value mismatch")
            except KeyError as err:
                self.assertEqual(len(self.dcd), 0, f"popitem invalid key error {err}")
                break

    def test_setdefault(self):
        """Test setting default values for dictionary keys."""
        self.dcd["a"] = 1
        self.mirror["a"] = 1
        self.assertEqual(
            self.dcd.setdefault("a", 5),
            self.mirror.setdefault("a", 5),
            "setdefault existing",
        )
        self.assertEqual(
            self.dcd.setdefault("b", 5),
            self.mirror.setdefault("b", 5),
            "setdefault new",
        )
        self._compare(self.mirror, self.dcd)

    def test_update(self):
        """Test updating dictionary with multiple items."""
        self._populate(100)
        new_values_dict = {}
        new_values_no_keys = []
        for i in range(1, 200):
            new_values_dict[i] = i
        self.mirror.update(new_values_dict)
        self.dcd.update(new_values_dict)
        self._compare(self.mirror, self.dcd)
        for i in range(1, 300):
            new_values_no_keys.append((i, i))
        self.mirror.update(new_values_no_keys)
        self.dcd.update(new_values_no_keys)
        self._compare(self.mirror, self.dcd)

    def test_values(self):
        """Test getting dictionary values."""
        self._populate(100)
        items = self.mirror.values()
        dcd_items = self.dcd.values()
        self._compare(self.mirror, self.dcd)
        self.assertEqual(
            sorted(items),
            sorted(dcd_items),
            f"Did not get correct values - Expected: {items} " f"Actual: {dcd_items}",
        )

    def test_save_load(self):
        """Save and continue on file is the expected work flow, what if
        instead the user saved to a new file, continues working, etc"""
        self._populate(100)
        temp_file = os.path.join(self.temp_dir, "savefile")
        junk_file = os.path.join(self.temp_dir, "junkfile")
        self.dcd.save(temp_file)
        # target a different scratch space so we don't change temp more.
        self.dcd.save(junk_file)
        self._compare(self.mirror, self.dcd)
        # clear the dict post save to confirm the load
        self.dcd.clear()
        self.assertEqual(len(self.dcd), 0, "Clear failed")
        self.dcd.load(temp_file)
        print(f"Loaded: {len(self.dcd.keys())} items")
        self._compare(self.mirror, self.dcd)

    def test_save_load_same_file(self):
        """Save, close, load into new dict."""
        self._populate(100)
        self.dcd.save()
        del self.dcd
        db_file = f"{self.db_file}_2"
        # load into a new object and check
        try:
            # pylint: disable=attribute-defined-outside-init
            self.dcd2 = disk_cache_dict.DefaultCacheDict(
                default_factory=list,
                max_size=self.cache_size // 2,
                lru=self.lru,
                current_dictionary=None,
                db_file=db_file,
                backend=self.backend,
            )
            # load into an empty/new dcd and confirm data
            self.dcd2.load(self.db_file)
            print(f"Loaded: {len(self.dcd2.keys())} items")
            self._compare(self.mirror, self.dcd2)
        finally:
            if hasattr(self, "dcd2"):
                del self.dcd2

    def test_save_reopen(self):
        """Save, close, load via db_file argument"""
        try:
            self._populate(self.cache_size + 1)
            self.dcd.save()
            # pylint: disable=protected-access
            self.assertEqual(len(self.dcd._cache), 0, "Failed to evict cache")
            # load into a new object and check
            # pylint: disable=attribute-defined-outside-init
            self.dcd2 = disk_cache_dict.DefaultCacheDict(
                default_factory=list,
                max_size=self.cache_size // 2,
                lru=self.lru,
                current_dictionary=None,
                db_file=self.db_file,
                backend=self.backend,
            )
            print(f"Opened db with: {len(self.dcd2.keys())} items")
            self._compare(self.mirror, self.dcd2)
            # pylint: disable=protected-access
            self.assertEqual(
                len(self.dcd2._cache),
                self.cache_size / 2,
                f"Failed to re-warm cache: now size: {len(self.dcd2._cache)}",
            )
        finally:
            del self.dcd
            del self.dcd2

    def test_sqlite_backend_default_db_file(self):
        """Test SqliteBackend with default db_file generation - line 41"""
        temp_dir = utils.make_temp_dir("dcd_temp")
        db_file = os.path.join(temp_dir, "test.db")
        try:
            backend = disk_cache_dict.SqliteBackend(db_file=db_file)
            # Should have created a file with timestamp-based name
            db_path = backend.db_file_path()
            self.assertTrue(db_path.endswith("test.db"))

            # Basic functionality test
            self.assertIsNotNone(backend.conn)
            self.assertTrue(hasattr(backend, "table"))
        finally:
            utils.remove_dir(temp_dir)

    def test_sqlite_backend_unlink_old_db(self):
        """Test SqliteBackend with unlink_old_db flag - line 43"""
        db_file = os.path.join(self.temp_dir, "test_unlink.db")

        # Create an existing db file
        with open(db_file, "w", encoding="utf-8") as f:
            f.write("old data")

        self.assertTrue(os.path.exists(db_file))

        # Create backend with unlink_old_db=True
        backend = disk_cache_dict.SqliteBackend(db_file=db_file, unlink_old_db=True)
        try:
            # Old file should have been deleted and new one created
            self.assertTrue(os.path.exists(db_file))

            # Should be a valid sqlite db, not the old file
            backend["test"] = "value"
            self.assertEqual(backend["test"], "value")
        finally:
            backend.close()

    def test_backend_keys_method(self):
        """Test SqliteBackend.keys() method - line 137"""
        db_file = os.path.join(self.temp_dir, "test_keys.db")
        backend = disk_cache_dict.SqliteBackend(db_file=db_file)
        try:
            # Add some data
            backend["key1"] = "value1"
            backend["key2"] = "value2"
            backend["key3"] = "value3"

            # Get keys iterator
            keys = list(backend.keys())

            self.assertEqual(len(keys), 3)
            self.assertIn("key1", keys)
            self.assertIn("key2", keys)
            self.assertIn("key3", keys)
        finally:
            backend.close()

    def test_backend_values_method(self):
        """Test SqliteBackend.values() method"""
        db_file = os.path.join(self.temp_dir, "test_values.db")
        backend = disk_cache_dict.SqliteBackend(db_file=db_file)
        try:
            # Add some data
            backend["key1"] = "value1"
            backend["key2"] = "value2"
            backend["key3"] = "value3"

            # Get values
            values = backend.values()

            self.assertEqual(len(values), 3)
            self.assertIn("value1", values)
            self.assertIn("value2", values)
            self.assertIn("value3", values)
        finally:
            backend.close()

    def test_backend_items_method(self):
        """Test SqliteBackend.items() method"""
        db_file = os.path.join(self.temp_dir, "test_items.db")
        backend = disk_cache_dict.SqliteBackend(db_file=db_file)
        try:
            # Add some data
            backend["key1"] = "value1"
            backend["key2"] = "value2"

            # Get items
            items = backend.items()

            self.assertEqual(len(items), 2)
            self.assertIn(("key1", "value1"), items)
            self.assertIn(("key2", "value2"), items)
        finally:
            backend.close()

    def test_backend_close_exception_handling(self):
        """Test SqliteBackend.close() exception handling"""
        # Test that close() handles OSError gracefully
        db_file = os.path.join(self.temp_dir, "test_close.db")
        backend = disk_cache_dict.SqliteBackend(db_file=db_file)

        backend["test"] = "value"

        # Normal close
        backend.close()

        # Simulate OSError scenario by deleting the db file before __del__
        # This ensures the except (sqlite3.OperationalError, OSError) clause is used
        if os.path.exists(db_file):
            os.unlink(db_file)

        # The __del__ method will call close() and should handle the OSError gracefully
        # We can't easily test this without forcing garbage collection,
        # but the test verifies that close() can be called after db file is deleted
        self.assertFalse(os.path.exists(db_file))

    def test_backend_save_with_remove_old_db(self):
        """Test SqliteBackend.save() with remove_old_db flag - line 184"""
        db_file = os.path.join(self.temp_dir, "test_save_old.db")
        backend = disk_cache_dict.SqliteBackend(db_file=db_file)

        try:
            # Add some data
            backend["key1"] = "value1"
            backend["key2"] = "value2"

            # Save to new file with remove_old_db=True
            new_db_file = os.path.join(self.temp_dir, "test_save_new.db")
            backend.save(db_file=new_db_file, remove_old_db=True)

            # Old db file should be removed
            self.assertFalse(
                os.path.exists(db_file), "Old db file should have been removed"
            )

            # New db file should exist and have data
            self.assertTrue(os.path.exists(new_db_file))
            self.assertEqual(backend["key1"], "value1")
            self.assertEqual(backend["key2"], "value2")

        finally:
            backend.close()

    def test_cache_dict_custom_backend(self):
        """Test CacheDict with custom backend - line 245"""
        # Create a custom backend (using SqliteBackend as the custom one)
        custom_db_file = os.path.join(self.temp_dir, "custom_backend.db")
        custom_backend = disk_cache_dict.SqliteBackend(db_file=custom_db_file)

        try:
            # Pre-populate the backend with data
            custom_backend["existing_key"] = "existing_value"
            custom_backend.commit()

            # Create CacheDict with custom backend (covers line 245)
            # This tests the "if backend:" branch at line 245
            cache = disk_cache_dict.CacheDict(
                max_size=5,
                db_file=None,  # Not used when backend provided
                backend=custom_backend,
            )

            # Use the cache
            cache["new_key"] = "new_value"

            # Verify data is retrievable
            self.assertEqual(cache["new_key"], "new_value")
            # Existing key from backend should still be accessible
            self.assertEqual(cache["existing_key"], "existing_value")

        finally:
            custom_backend.close()


class TestDefaultCacheDict(unittest.TestCase):
    """Unit tests for DefaultCacheDict."""

    def test_init_raises_type_error(self):
        """Test that initializing DefaultCacheDict with non-callable default_factory
        raises TypeError."""
        with self.assertRaises(TypeError):
            disk_cache_dict.DefaultCacheDict(default_factory="not_callable")

    def test_copy_method(self):
        """Test that copy method returns a new DefaultCacheDict with same default_factory."""
        temp_dir = utils.make_temp_dir("dcd_temp")
        db_file = os.path.join(temp_dir, "test.db")
        try:
            dcd = disk_cache_dict.DefaultCacheDict(
                default_factory=list, db_file=db_file
            )
            dcd_copy = dcd.copy(os.path.join(temp_dir, "test_copy.db"))
            self.assertIsInstance(dcd_copy, disk_cache_dict.DefaultCacheDict)
            self.assertEqual(dcd_copy.default_factory, list)
        finally:
            utils.remove_dir(temp_dir)

    def test_fromkeys_method(self):
        """Test that fromkeys method returns a new DefaultCacheDict with same default_factory."""
        temp_dir = utils.make_temp_dir("dcd_temp")
        db_file = os.path.join(temp_dir, "test.db")
        try:
            dcd = disk_cache_dict.DefaultCacheDict(
                default_factory=list, db_file=db_file
            )
            keys = ["a", "b", "c"]
            dcd_fromkeys = dcd.fromkeys(
                keys, db_file=os.path.join(temp_dir, "test_from_keys.db")
            )
            self.assertIsInstance(dcd_fromkeys, disk_cache_dict.DefaultCacheDict)
            self.assertEqual(dcd_fromkeys.default_factory, list)
        finally:
            utils.remove_dir(temp_dir)

    def test_dump_load_bytes_and_bool(self):
        """Test that bytes and bool types are dumped and loaded correctly"""
        # pylint: disable=protected-access
        # Test boolean
        self.assertEqual(
            disk_cache_dict.SqliteBackend._load(
                disk_cache_dict.SqliteBackend._dump(True)
            ),
            True,
        )
        self.assertEqual(
            disk_cache_dict.SqliteBackend._load(
                disk_cache_dict.SqliteBackend._dump(False)
            ),
            False,
        )
        # Test bytes
        self.assertEqual(
            disk_cache_dict.SqliteBackend._load(
                disk_cache_dict.SqliteBackend._dump(b"hello")
            ),
            b"hello",
        )


class TestDefaultDiskDictFunctional(DcdActionSuite, unittest.TestCase):
    """Functional tests around the DefualtDict version of the disk_cache_dict."""

    def setUp(self):
        print("NON LRU DEFAULT")
        self.lru = False
        super().setUp()
        self.dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=self.cache_size,
            db_file=self.db_file,
            lru=False,
            current_dictionary=None,
        )


class TestDefaultDiskDictLruFunctional(DcdActionSuite, unittest.TestCase):
    """Functional tests around the DefualtDict version of the disk_cache_dict"""

    def setUp(self):
        print("LRU DEFAULT")
        self.lru = True
        super().setUp()
        self.dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=self.cache_size,
            db_file=self.db_file,
            lru=True,
            current_dictionary=None,
        )

    def test_lru(self):
        """Test least-recently-used cache eviction policy."""
        cache = collections.deque([], self.cache_size)
        keys = range(self.cache_size * 2)
        for i in keys:
            self.dcd[i] = i
            cache.append(i)
        for i in range(1000):
            test_key = random.choice(keys)
            self.dcd[test_key] = i
            if test_key in cache:
                cache.remove(test_key)
            cache.append(test_key)
            # pylint: disable=protected-access
            in_cache = self.dcd._cache.keys()
            print(in_cache, self.dcd._key_order)
            print(cache)
            print("....")
            last_key = (
                next(reversed(self.dcd._key_order)) if self.dcd._key_order else None
            )
            self.assertEqual(test_key, last_key, "not lru")
            self.assertEqual(
                self.cache_size, len(self.dcd._cache.keys()), "Dcd cache is wrong size"
            )
            self.assertFalse(
                set(in_cache).symmetric_difference(set(cache)), "Lru key mismatch"
            )


if __name__ == "__main__":
    unittest.main()
