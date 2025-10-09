"""A LRUish cache / disk backed  (via a database) dictionary
implementation

Currently uses sqlite3 as the backend.

Note: Future enhancements could include:
    - Document backend requirements and interface
    - Implement full LRU eviction policy (currently uses random eviction)
"""

import collections.abc
import os
import pickle
import sqlite3
import sys
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

IS_WIN = sys.platform == "win32"
DEBUG = False


class SqliteBackend:
    """A thread-safe SQLite backend using a single connection with a lock.
    Query/update scheme adapted from Erez Shinan's filedict project.
    """

    def __init__(
        self,
        db_file: Optional[str] = None,
        db_table: str = "sql_dict_table",
        unlink_old_db: bool = False,
    ) -> None:
        """Create the db and tables"""
        if db_file is None:
            db_file = f"db_file_{int(time.time())}.dict"
        if unlink_old_db and os.path.exists(db_file):
            os.unlink(db_file)
        self._db_file = db_file
        self.table = db_table
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_conn()
        self._commit_needed = False
        self._write_count = 0
        self._batch_size = 100

    def _init_conn(self) -> None:
        """Initializes a single, shared connection."""
        with self._lock:
            if self._conn is None:
                self._conn = sqlite3.connect(
                    self._db_file, check_same_thread=False, timeout=10
                )
                self._conn.execute("PRAGMA journal_mode=WAL;")
                self._conn.execute("PRAGMA synchronous=NORMAL;")
                self._conn.execute("PRAGMA cache_size = -64000;")
                self._conn.execute(
                    f"create table if not exists {self.table} (id integer primary "
                    f"key, hash integer, key blob, value blob);"
                )
                self._conn.execute(
                    f"create index if not exists {self.table}_index ON {self.table}(hash);"
                )
                self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        """Return the shared SQLite connection."""
        if self._conn is None:
            self._init_conn()
        return self._conn  # type: ignore

    def _get_key_id(self, key: Any) -> int:
        """Get the database ID for a given key, or raise KeyError if not found."""
        with self._lock:
            cursor = self.conn.execute(
                f"select key,id from {self.table} where hash=?;", (hash(key),)
            )
            for k, key_id in cursor:
                if self._load(k) == key:
                    return key_id
            raise KeyError(key)

    def __getitem__(self, key: Any) -> Any:
        """Get item from the dictionary."""
        with self._lock:
            cursor = self.conn.execute(
                f"select key,value from {self.table} where hash=?;", (hash(key),)
            )
            for k, v in cursor:
                if self._load(k) == key:
                    return self._load(v)
            raise KeyError(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        """Set item in the dictionary."""
        with self._lock:
            self._insert(key, value)
            self._commit_needed = True
            self._write_count += 1
            if self._write_count >= self._batch_size:
                self.commit()

    def __delitem__(self, key: Any) -> None:
        """Delete item from the dictionary."""
        with self._lock:
            db_id = self._get_key_id(key)
            self.conn.execute(f"delete from {self.table} where id=?;", (db_id,))
            self._commit_needed = True
            self._write_count += 1
            if self._write_count >= self._batch_size:
                self.commit()

    def __iter__(self) -> Iterator[Any]:
        """Return an iterator over the keys of the dictionary."""
        with self._lock:
            # Fetch all keys at once to avoid lock contention during iteration
            keys = [
                self._load(k[0])
                for k in self.conn.execute(f"select key from {self.table};")
            ]
        return iter(keys)

    def __len__(self) -> int:
        """Return the number of items in the dictionary."""
        with self._lock:
            return self.conn.execute(f"select count(*) from {self.table};").fetchone()[
                0
            ]

    def __contains__(self, key: Any) -> bool:
        """Check if key exists in the dictionary."""
        try:
            self._get_key_id(key)
            return True
        except KeyError:
            return False

    @staticmethod
    def _dump(value: Any, version: int = -1) -> bytes:
        """Serialize value for storage in the database."""
        # Fast path for primitive types - avoid pickle overhead
        if isinstance(value, str):
            return sqlite3.Binary(b"S" + value.encode("utf-8"))
        if isinstance(value, int):
            return sqlite3.Binary(b"I" + str(value).encode("utf-8"))
        if isinstance(value, float):
            return sqlite3.Binary(b"F" + str(value).encode("utf-8"))
        if isinstance(value, bool):
            return sqlite3.Binary(b"B" + (b"1" if value else b"0"))
        if value is None:
            return sqlite3.Binary(b"N")
        # Fall back to pickle for complex types
        return sqlite3.Binary(b"P" + pickle.dumps(value, version))

    # pylint: disable=too-many-return-statements
    @staticmethod
    def _load(value: bytes) -> Any:
        """Inverse of _dump."""
        value_bytes = bytes(value)
        if not value_bytes:
            return None
        # Check type marker
        type_marker = value_bytes[0:1]
        if type_marker == b"S":
            return value_bytes[1:].decode("utf-8")
        if type_marker == b"I":
            return int(value_bytes[1:].decode("utf-8"))
        if type_marker == b"F":
            return float(value_bytes[1:].decode("utf-8"))
        if type_marker == b"B":
            return value_bytes[1:] == b"1"
        if type_marker == b"N":
            return None
        if type_marker == b"P":
            return pickle.loads(value_bytes[1:])
        # Legacy: no type marker, assume pickle
        return pickle.loads(value_bytes)

    def _insert(self, key: Any, value: Any) -> None:
        """Assumes lock is held."""
        try:
            db_id = self._get_key_id(key)
            self.conn.execute(
                f"update {self.table} set value=? where id=?;",
                (self._dump(value), db_id),
            )
        except KeyError:
            self.conn.execute(
                f"insert into {self.table} (hash, key, value) values (?, ?, ?);",
                (hash(key), self._dump(key), self._dump(value)),
            )

    def pop(self, key: Any) -> Any:
        """Remove specified key and return the corresponding value.
        Raises KeyError if key is not found.
        """
        with self._lock:
            value = self[key]
            del self[key]
            return value

    def keys(self) -> Iterator[Any]:
        """Return an iterator over the keys of the dictionary."""
        return iter(self)

    def values(self) -> List[Any]:
        """Return a list of all values in the dictionary."""
        with self._lock:
            return [
                self._load(x[0])
                for x in self.conn.execute(f"select value from {self.table};")
            ]

    def items(self) -> List[Tuple[Any, Any]]:
        """Return a list of all key-value pairs in the dictionary."""
        with self._lock:
            return [
                (self._load(items[0]), self._load(items[1]))
                for items in self.conn.execute(f"select key,value from {self.table};")
            ]

    def commit(self, force: bool = False) -> None:
        """Commit any pending transactions to the database."""
        with self._lock:
            if self._commit_needed or force:
                self.conn.commit()
            self._commit_needed = False
            self._write_count = 0

    def db_file_path(self) -> str:
        """Return the db file path if there is one"""
        return self._db_file

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if self._conn:
                try:
                    self.commit(force=True)
                    self._conn.close()
                except (sqlite3.OperationalError, sqlite3.ProgrammingError):
                    pass
            self._conn = None

    def __del__(self) -> None:
        """Destructor to ensure the database connection is closed."""
        try:
            self.close()
        except Exception:  # pylint: disable=W0718
            pass

    def save(self, db_file: Optional[str] = None, remove_old_db: bool = False) -> None:
        """Save to a new database file, optionally removing the old one."""
        with self._lock:
            self.commit(force=True)
            current_db_file = self._db_file
            if db_file is not None and db_file != current_db_file:
                dest_conn = sqlite3.connect(db_file)
                with dest_conn:
                    self.conn.backup(dest_conn)
                dest_conn.close()
                if remove_old_db:
                    self.close()
                    os.unlink(current_db_file)
                    self._db_file = db_file
                    self._init_conn()

    def load(self, db_file: Optional[str] = None) -> None:
        """Load from a different database file."""
        with self._lock:
            self.commit(force=True)
            self.close()
            db_file = db_file or self._db_file
            self._db_file = db_file
            self._init_conn()
            self._commit_needed = False
            self._write_count = 0


# Container stuff
class CacheDict(collections.abc.MutableMapping):
    """Acts like a normal dictionary until we hit a maximum size, and then
    items are stored in a backing database in LRU or random fashion.

    There are no guarantees around thread safety, so be careful.

    Items are removed from DB when pulled into cache, and written back when
    they fall out of the cache.

    Note: Current design uses cache-exclusive model (items either in cache OR db).
    An alternative design could keep items in both cache and db simultaneously.
    """

    def __init__(
        self,
        max_size: int = 100000,
        db_file: Optional[str] = None,
        *,
        backend: Optional[Any] = None,
        lru: bool = False,
        current_dictionary: Optional[Dict[Any, Any]] = None,
    ) -> None:
        self._evict_lock_held = False
        self._cache: Dict[Any, Any] = {}
        self._db_file = db_file
        if backend:
            self._db = backend
        else:
            self._db = SqliteBackend(db_file)
        self.max_size = max_size
        self.lru = lru
        self._key_order: Optional[OrderedDict] = None
        if lru:
            self._key_order = OrderedDict()
        # Note: Performance could be improved with a batched option
        if current_dictionary:
            for key, value in current_dictionary.items():
                self[key] = value

    def __contains__(self, key: Any) -> bool:
        """Check for existence in local cache, fall back to db.

        Note: Currently, checking existence will fault the item into cache if found in db.
        """
        local = key in self._cache
        if not local:
            return key in self._db
        return local

    def __len__(self) -> int:
        """Sum the len of every mapping"""
        return len(self._cache) + len(self._db)

    def __iter__(self) -> Iterator[Any]:
        """Get the keys from local and the db"""
        # this method must account for items faulting in and out of cache
        # in the iteritems case. While iterating, the cache is frozen.
        try:
            self._evict_lock_held = True
            yield from self._cache
            yield from self._db
        finally:
            self._evict_lock_held = False

    def __getitem__(self, key: Any) -> Any:
        """Returns the stored item at key from a sub mapping or
        raises KeyError"""
        value = None
        if key in self._cache:
            value = self._cache[key]
        else:
            # no fault if we can't evict
            if self._evict_lock_held:
                return self._db[key]

            value = self._fault(key)
        if self.lru and self._key_order is not None:
            # O(1) operation - track access order
            if key in self._key_order:
                self._key_order.move_to_end(key, last=True)
            else:
                self._key_order[key] = None
        return value

    # the type here is not ItemsView - may look into aligning
    # with parent class at a later date
    def items(self) -> Iterator[Tuple[Any, Any]]:  # type: ignore[override]
        """Return an iterator over the items of the dictionary.

        This is a non-destructive iterator. It will not fault items
        into the cache.
        """
        # First, yield all items from the in-memory cache.
        yield from self._cache.items()

        # Then, iterate over items in the database backend.
        # For each item, if its key is NOT already in the cache (which we just yielded),
        # then yield it. This avoids yielding duplicate keys.
        # This implementation assumes self._db.items() is an efficient way
        # to iterate over all items in the backend storage.
        db_items = self._db.items()
        for key, value in db_items:
            if key not in self._cache:
                yield key, value

    def __setitem__(self, key: Any, value: Any) -> None:
        """Put an items into the mappings, if key doesn't exist, create it"""
        if key not in self._cache and key in self._db:
            # no fault if we can't evict, just update db
            if self._evict_lock_held:
                self._db[key] = value
                return
            # dump the db value and assign in cache
            del self._db[key]
        if key not in self._cache:
            if self._evict_lock_held:
                self._db[key] = value
            else:
                self._evict()
        self._cache[key] = value
        if self._evict_lock_held:
            return
        if self.lru and self._key_order is not None:
            # O(1) operation - add if new, or move to end if exists
            if key in self._key_order:
                self._key_order.move_to_end(key, last=True)
            else:
                self._key_order[key] = None

    def __delitem__(self, key: Any) -> None:
        """Remove item, raise KeyError if does not exist"""
        if key in self._cache:
            del self._cache[key]
        else:
            del self._db[key]
        if self.lru and self._key_order is not None:
            self._key_order.pop(key, None)

    def _fault(self, key: Any) -> Any:
        """Bring key in from db or raise KeyError, trigger evict if over
        size constraints"""
        # fetch from db and put in local cache
        value = self._db.pop(key)
        # make room in local cache
        self._evict()
        self._cache[key] = value
        return value

    def _evict(self) -> None:
        """Push oldest key out of local cache to db if exceeding size limit"""
        if len(self._cache) < self.max_size:
            return
        if self.lru and self._key_order is not None:
            # O(1) operation - get and remove oldest key
            key = next(iter(self._key_order))
            self._key_order.pop(key)
        else:
            # dump a semi-random key
            view_iter = iter(self._cache.keys())
            key = next(view_iter)
        # take the key out of cache and put in in db
        value = self._cache.pop(key)
        self._db[key] = value

    def get(self, key: Any, default: Any = None) -> Any:
        """Get item or default if not found"""
        if key not in self:
            return default
        return self[key]

    def has_key(self, key: Any) -> bool:
        """Check if key exists in the dictionary (deprecated method)."""
        return key in self

    def copy(self, db_file: Optional[str] = None) -> "CacheDict":
        """Returns a dictionary as a shallow from the cache dict"""
        newcd = CacheDict(
            max_size=self.max_size, backend=self._db, lru=self.lru, db_file=db_file
        )
        newcd.update(self)
        return newcd

    def fromkeys(
        self, keys: Iterator[Any], default: Any = None, db_file: Optional[str] = None
    ) -> "CacheDict":
        """Create a new CacheDict with keys from iterable and values set to default."""
        newcd = CacheDict(
            max_size=self.max_size, backend=self._db, lru=self.lru, db_file=db_file
        )
        for key in keys:
            newcd[key] = default
        return newcd

    def db_file_path(self) -> str:
        """Return the db file path if there is one"""
        return self._db.db_file_path()

    def load(self, db_file: Optional[str] = None) -> None:
        """Load from a db instance"""
        # clear out local cache so we're correctly in sync
        self._cache.clear()
        if self.lru:
            self._key_order = OrderedDict()
        self._db.load(db_file=db_file)

    def save(self, db_file: Optional[str] = None, remove_old_db: bool = False) -> None:
        """Dump all to database"""
        for key, value in self._cache.items():
            self._db[key] = value
        # Force commit any pending writes
        self._db.commit(force=True)
        if self.lru:
            self._key_order = OrderedDict()
        # clear out local cache so we're correctly in sync
        self._db.save(db_file=db_file or self._db_file, remove_old_db=remove_old_db)
        self._cache.clear()

    def close(self) -> None:
        """Close the underlying database connection."""
        if hasattr(self._db, "close"):
            self._db.close()


class DefaultCacheDict(CacheDict):
    """A CacheDict that provides default values for missing keys."""

    def __init__(
        self, default_factory: Optional[Callable[[], Any]] = None, **kwargs: Any
    ) -> None:
        self._kwargs = kwargs
        if default_factory is not None and not hasattr(default_factory, "__call__"):
            raise TypeError("the factory must be callable")
        super().__init__(**kwargs)
        self.default_factory = default_factory

    def __getitem__(self, key: Any) -> Any:
        try:
            return super().__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key: Any) -> Any:
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
            return default
        return self[key]

    def copy(self, db_file: Optional[str] = None) -> "DefaultCacheDict":
        """Returns a dictionary as a shallow from the cache dict"""
        new_kwargs: Dict[str, Any] = {}
        for key, value in self._kwargs.items():
            new_kwargs[key] = value
        new_kwargs["db_file"] = db_file
        newcd = DefaultCacheDict(default_factory=self.default_factory, **new_kwargs)
        newcd.update(self)
        return newcd

    def fromkeys(
        self, keys: Iterator[Any], default: Any = None, db_file: Optional[str] = None
    ) -> "DefaultCacheDict":
        new_kwargs: Dict[str, Any] = {}
        for origkey, origvalue in self._kwargs.items():
            new_kwargs[origkey] = origvalue
        new_kwargs["db_file"] = db_file
        newcd = DefaultCacheDict(default_factory=self.default_factory, **new_kwargs)
        for key in keys:
            newcd[key] = default
        return newcd


# Note: Cleanup of temporary dictionary files is currently handled by caller.
# A future enhancement could add a context manager or explicit cleanup helper.
