"""Property-based tests using Hypothesis for DedupeCopy core invariants.

These tests are excluded from the default fast CI test run (`pytest`) via the
`hypothesis` marker in `pyproject.toml`, and are intended to be run before
releases or when modifying core data structures and path/deletion logic:

    pytest -m hypothesis
"""

import collections
import math
import os
import queue
import tempfile
import threading
import unittest
from typing import Any, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    initialize,
    invariant,
    rule,
)

from dedupe_copy.config import DeleteJob
from dedupe_copy.core import _collect_files_to_delete, _select_files_for_deletion
from dedupe_copy.disk_cache_dict import (
    CacheDict,
    DefaultCacheDict,
    PersistentSet,
    _deserialize,
    _serialize,
)
from dedupe_copy.path_rules import strip_read_path_prefix
from dedupe_copy.threads import ResultProcessor
from dedupe_copy.utils import ExtensionMatcher, clean_extensions, match_extension

pytestmark = pytest.mark.hypothesis

# pylint: disable=protected-access


# Strategies for values stored in SqliteBackend / CacheDict
scalar_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63) + 1, max_value=2**63 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=120),
    st.binary(max_size=120),
)

file_entry_strategy = st.tuples(
    st.text(
        alphabet=st.characters(blacklist_categories=["Cs"], blacklist_characters="\x00"),
        min_size=1,
        max_size=60,
    ),
    st.integers(min_value=0, max_value=10**12),
    st.floats(min_value=0.0, max_value=2e9, allow_nan=False, allow_infinity=False),
)

serializable_values = st.one_of(
    scalar_values,
    st.lists(file_entry_strategy, max_size=10),
    st.dictionaries(st.text(max_size=20), scalar_values, max_size=5),
)


class TestSerializationProperties(unittest.TestCase):
    """Property-based tests for _serialize and _deserialize."""

    @given(value=serializable_values)
    @settings(max_examples=200, deadline=None)
    def test_serialize_deserialize_roundtrip(self, value: Any) -> None:
        """Any supported value should round-trip through _serialize/_deserialize."""
        dumped = _serialize(value)
        self.assertIsInstance(dumped, bytes)
        restored = _deserialize(dumped)
        if isinstance(value, float):
            self.assertTrue(math.isclose(restored, value, rel_tol=1e-12, abs_tol=0.0))
        else:
            self.assertEqual(restored, value)
            if isinstance(value, bool):
                self.assertIsInstance(restored, bool)


class TestExtensionProperties(unittest.TestCase):
    """Property-based tests for extension cleaning and matching."""

    @given(
        exts=st.lists(
            st.text(
                alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-",
                min_size=1,
                max_size=12,
            ),
            min_size=1,
            max_size=15,
        ),
        stem=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
            min_size=1,
            max_size=20,
        ),
        ext=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            min_size=1,
            max_size=8,
        ),
    )
    @settings(max_examples=150, deadline=None)
    def test_clean_extensions_and_matcher_equivalence(
        self, exts: list[str], stem: str, ext: str
    ) -> None:
        """clean_extensions is idempotent and ExtensionMatcher matches match_extension."""
        cleaned_once = clean_extensions(exts)
        cleaned_twice = clean_extensions(cleaned_once)
        self.assertEqual(cleaned_once, cleaned_twice)
        for item in cleaned_once:
            self.assertTrue(item.startswith("."))
            self.assertEqual(item, item.lower())

        filename = f"/tmp/dir/{stem}.{ext}"
        matcher = ExtensionMatcher(exts)
        self.assertEqual(
            matcher.match(filename),
            match_extension(cleaned_once, filename),
        )


class TestPathPrefixProperties(unittest.TestCase):
    """Property-based tests for strip_read_path_prefix."""

    segment_strategy = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
        min_size=1,
        max_size=10,
    )

    @given(
        base_segments=st.lists(segment_strategy, min_size=1, max_size=3),
        sub_segments=st.lists(segment_strategy, min_size=1, max_size=3),
        rel_segments=st.lists(segment_strategy, min_size=1, max_size=3),
        filename=segment_strategy,
    )
    @settings(max_examples=150, deadline=None)
    def test_nested_read_paths_prefer_longest_boundary_match(
        self,
        base_segments: list[str],
        sub_segments: list[str],
        rel_segments: list[str],
        filename: str,
    ) -> None:
        """Nested read paths always strip the most specific read path prefix."""
        outer_root = os.sep + os.path.join(*base_segments)
        inner_root = os.path.join(outer_root, *sub_segments)
        target_file = os.path.join(inner_root, *rel_segments, f"{filename}.txt")
        expected_rel = os.path.join(*rel_segments, f"{filename}.txt")

        # Regardless of the order of read_paths, the longest matching root wins
        stripped_a, matched_a = strip_read_path_prefix(
            target_file, [outer_root, inner_root]
        )
        stripped_b, matched_b = strip_read_path_prefix(
            target_file, [inner_root, outer_root]
        )
        self.assertTrue(matched_a)
        self.assertTrue(matched_b)
        self.assertEqual(stripped_a, expected_rel)
        self.assertEqual(stripped_b, expected_rel)

    @given(
        base_segments=st.lists(segment_strategy, min_size=1, max_size=3),
        suffix=segment_strategy,
        filename=segment_strategy,
    )
    @settings(max_examples=100, deadline=None)
    def test_sibling_prefix_never_partially_stripped(
        self, base_segments: list[str], suffix: str, filename: str
    ) -> None:
        """A read path '/a/b' must never strip a sibling directory '/a/b_suffix/file'."""
        read_root = os.sep + os.path.join(*base_segments)
        sibling_root = f"{read_root}_{suffix}"
        sibling_file = os.path.join(sibling_root, f"{filename}.txt")
        rel_path, matched = strip_read_path_prefix(sibling_file, [read_root])
        self.assertFalse(matched)
        self.assertEqual(
            rel_path, os.path.normpath(sibling_file).lstrip(os.sep)
        )


class TestMergeAndDeletionSafetyProperties(unittest.TestCase):
    """Property-based tests for ResultProcessor hash merging and deletion safety."""

    @given(
        rel_paths=st.lists(
            st.sampled_from(
                [
                    "a/file1.bin",
                    "a/./file1.bin",
                    "b/file2.bin",
                    "c/file3.bin",
                    "b/../a/file1.bin",
                ]
            ),
            min_size=1,
            max_size=20,
        ),
        split_idx=st.integers(min_value=1, max_value=19),
    )
    @settings(max_examples=100, deadline=None)
    def test_merge_files_for_hash_batch_invariance(
        self, rel_paths: list[str], split_idx: int
    ) -> None:
        """Merging files across multiple batches yields the same deduplicated set as one batch."""
        entries = [(p, 100, float(i)) for i, p in enumerate(rel_paths)]

        proc_single = ResultProcessor(
            threading.Event(), queue.Queue(), {}, {}
        )
        merged_single, _ = proc_single._merge_files_for_hash(
            "md5_key", entries, already_existed=False, existing_files=[]
        )

        proc_multi = ResultProcessor(
            threading.Event(), queue.Queue(), {}, {}
        )
        part1 = entries[:split_idx]
        part2 = entries[split_idx:]
        curr, _ = proc_multi._merge_files_for_hash(
            "md5_key", part1, already_existed=False, existing_files=[]
        )
        if part2:
            curr, _ = proc_multi._merge_files_for_hash(
                "md5_key", part2, already_existed=bool(curr), existing_files=curr
            )

        norm_single = {os.path.normcase(os.path.abspath(f[0])): f for f in merged_single}
        norm_multi = {os.path.normcase(os.path.abspath(f[0])): f for f in curr}
        self.assertEqual(len(merged_single), len(norm_single))
        self.assertEqual(norm_single, norm_multi)

    @given(
        distinct_names=st.lists(
            st.sampled_from(["f1.dat", "f2.dat", "f3.dat", "f4.dat", "f5.dat"]),
            min_size=1,
            max_size=12,
        ),
        size=st.integers(min_value=0, max_value=10000),
        min_delete_size=st.integers(min_value=0, max_value=5000),
        dedupe_empty=st.booleans(),
    )
    @settings(max_examples=150, deadline=None)
    def test_select_files_for_deletion_never_deletes_all_physical_files(
        self,
        distinct_names: list[str],
        size: int,
        min_delete_size: int,
        dedupe_empty: bool,
    ) -> None:
        """Without a compare manifest, at least one physical file must always survive."""
        base_dir = "/tmp/hyp_del_test"
        file_list: list[tuple[str, int, float]] = []
        for idx, name in enumerate(distinct_names):
            # Mix canonical paths and redundant relative segments to test physical path deduping
            if idx % 2 == 0:
                path = os.path.join(base_dir, name)
            else:
                path = os.path.join(base_dir, "sub", "..", name)
            file_list.append((path, size, 1000.0 + idx))

        selected = _select_files_for_deletion(file_list, delete_all=False)
        delete_job = DeleteJob(
            dry_run=True,
            min_delete_size_bytes=min_delete_size,
            dedupe_empty=dedupe_empty,
        )
        files_to_delete = _collect_files_to_delete(
            duplicates={"hash1": file_list},
            delete_job=delete_job,
            hashes_to_delete_all=set(),
            progress_queue=None,
        )

        all_physical = {os.path.normcase(os.path.abspath(f[0])) for f in file_list}
        selected_physical = {os.path.normcase(os.path.abspath(f[0])) for f in selected}
        deleted_physical = {os.path.normcase(os.path.abspath(p)) for p in files_to_delete}
        self.assertGreaterEqual(len(all_physical - selected_physical), 1)
        self.assertGreaterEqual(
            len(all_physical - deleted_physical),
            1,
            f"All physical files were selected for deletion! "
            f"all={all_physical}, deleted={deleted_physical}",
        )


class CacheDictStateMachine(RuleBasedStateMachine):
    """Stateful property test comparing DefaultCacheDict against collections.defaultdict."""

    keys = Bundle("keys")

    def __init__(self) -> None:
        super().__init__()
        self.temp_dir = tempfile.mkdtemp(prefix="hyp_dcd_")
        self.db_file = os.path.join(self.temp_dir, "state.dict")
        self.oracle: dict[str, Any] = collections.defaultdict(list)
        self.dcd: Optional[DefaultCacheDict] = None

    @initialize(
        max_size=st.integers(min_value=2, max_value=8),
        lru=st.booleans(),
    )
    def init_dict(self, max_size: int, lru: bool) -> None:
        """Initialize DefaultCacheDict with small cache size to force frequent evictions."""
        self.dcd = DefaultCacheDict(
            default_factory=list,
            max_size=max_size,
            db_file=self.db_file,
            lru=lru,
        )
        self.dcd._db._batch_size = 4

    @rule(target=keys, k=st.text(alphabet="abcdefghij", min_size=1, max_size=4))
    def gen_key(self, k: str) -> str:
        """Provide keys for stateful operations."""
        return k

    @rule(k=keys, v=st.lists(st.integers(min_value=0, max_value=100), max_size=4))
    def set_item(self, k: str, v: list[int]) -> None:
        """Test __setitem__."""
        assert self.dcd is not None
        self.oracle[k] = list(v)
        self.dcd[k] = list(v)

    @rule(k=keys)
    def get_item(self, k: str) -> None:
        """Test __getitem__ (including DefaultCacheDict auto-insertion)."""
        assert self.dcd is not None
        expected = self.oracle[k]
        actual = self.dcd[k]
        assert actual == expected

    @rule(k=keys, default=st.integers(min_value=-10, max_value=-1))
    def get_with_default(self, k: str, default: int) -> None:
        """Test .get(k, default) does not insert missing keys."""
        assert self.dcd is not None
        expected = self.oracle.get(k, default)
        actual = self.dcd.get(k, default)
        assert actual == expected

    @rule(k=keys, default=st.lists(st.integers(min_value=100, max_value=200), max_size=2))
    def setdefault_item(self, k: str, default: list[int]) -> None:
        """Test .setdefault(k, default)."""
        assert self.dcd is not None
        expected = self.oracle.setdefault(k, list(default))
        actual = self.dcd.setdefault(k, list(default))
        assert actual == expected

    @rule(k=keys)
    def delete_item(self, k: str) -> None:
        """Test __delitem__."""
        assert self.dcd is not None
        if k in self.oracle:
            del self.oracle[k]
            del self.dcd[k]
        else:
            with pytest.raises(KeyError):
                del self.dcd[k]

    @rule(
        batch=st.dictionaries(
            st.text(alphabet="abcdefghij", min_size=1, max_size=4),
            st.lists(st.integers(min_value=0, max_value=50), max_size=3),
            max_size=10,
        )
    )
    def update_batch_items(self, batch: dict[str, list[int]]) -> None:
        """Test update_batch."""
        assert self.dcd is not None
        clean_batch = {k: list(v) for k, v in batch.items()}
        self.oracle.update(clean_batch)
        self.dcd.update_batch(clean_batch)

    @rule()
    def save_and_reload(self) -> None:
        """Test save() followed by load()."""
        assert self.dcd is not None
        self.dcd.save()
        self.dcd.load()

    @invariant()
    def check_consistency(self) -> None:
        """Verify cache/DB disjointness, length, and key/value equivalence."""
        if self.dcd is None:
            return
        cache_keys = set(self.dcd._cache.keys())
        db_keys = set(self.dcd._db.keys())
        assert cache_keys.isdisjoint(
            db_keys
        ), f"Cache and DB keys overlap: {cache_keys & db_keys}"
        assert len(self.dcd._cache) <= self.dcd.max_size
        assert len(self.dcd) == len(self.oracle)
        assert dict(self.dcd.items()) == dict(self.oracle)

    def teardown(self) -> None:
        """Clean up temporary database files."""
        if self.dcd is not None:
            self.dcd.close()
        for root, _, files in os.walk(self.temp_dir, topdown=False):
            for f in files:
                try:
                    os.unlink(os.path.join(root, f))
                except OSError:
                    pass
            try:
                os.rmdir(root)
            except OSError:
                pass


TestCacheDictStateMachine = CacheDictStateMachine.TestCase
TestCacheDictStateMachine.settings = settings(
    max_examples=25,
    stateful_step_count=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


class PersistentSetStateMachine(RuleBasedStateMachine):
    """Stateful property test comparing PersistentSet against Python set."""

    items = Bundle("items")

    def __init__(self) -> None:
        super().__init__()
        self.temp_dir = tempfile.mkdtemp(prefix="hyp_pset_")
        self.db_file = os.path.join(self.temp_dir, "pset.db")
        self.oracle: set[str] = set()
        self.pset: Optional[PersistentSet] = None

    @initialize(max_size=st.integers(min_value=2, max_value=8))
    def init_set(self, max_size: int) -> None:
        """Initialize PersistentSet with small cache size and small batch size."""
        self.pset = PersistentSet(max_size=max_size, db_file=self.db_file)
        self.pset._db._batch_size = 4

    @rule(target=items, v=st.text(alphabet="abcdefghij", min_size=1, max_size=5))
    def gen_item(self, v: str) -> str:
        """Generate set elements."""
        return v

    @rule(v=items)
    def add_item(self, v: str) -> None:
        """Test add()."""
        assert self.pset is not None
        self.oracle.add(v)
        self.pset.add(v)

    @rule(v=items)
    def discard_item(self, v: str) -> None:
        """Test discard()."""
        assert self.pset is not None
        self.oracle.discard(v)
        self.pset.discard(v)

    @rule(
        batch=st.lists(
            st.text(alphabet="abcdefghij", min_size=1, max_size=5), max_size=10
        )
    )
    def update_items(self, batch: list[str]) -> None:
        """Test update()."""
        assert self.pset is not None
        self.oracle.update(batch)
        self.pset.update(batch)

    @rule(
        batch=st.lists(
            st.text(alphabet="abcdefghij", min_size=1, max_size=5), max_size=6
        )
    )
    def discard_batch_items(self, batch: list[str]) -> None:
        """Test discard_batch()."""
        assert self.pset is not None
        self.oracle.difference_update(batch)
        self.pset.discard_batch(batch)

    @rule()
    def save_and_reload(self) -> None:
        """Test save() and load()."""
        assert self.pset is not None
        self.pset.save()
        self.pset.load()

    @rule()
    def clear_all(self) -> None:
        """Test clear()."""
        assert self.pset is not None
        self.oracle.clear()
        self.pset.clear()

    @invariant()
    def check_consistency(self) -> None:
        """Verify cache/DB disjointness, length, and element equivalence."""
        if self.pset is None:
            return
        db_items = set(self.pset._db)
        assert self.pset._cache.isdisjoint(
            db_items
        ), f"PersistentSet cache and DB overlap: {self.pset._cache & db_items}"
        assert len(self.pset) == len(self.oracle)
        assert set(self.pset) == self.oracle
        for elem in self.oracle:
            assert elem in self.pset

    def teardown(self) -> None:
        """Clean up temporary database files."""
        if self.pset is not None:
            self.pset.close()
        for root, _, files in os.walk(self.temp_dir, topdown=False):
            for f in files:
                try:
                    os.unlink(os.path.join(root, f))
                except OSError:
                    pass
            try:
                os.rmdir(root)
            except OSError:
                pass


TestPersistentSetStateMachine = PersistentSetStateMachine.TestCase
TestPersistentSetStateMachine.settings = settings(
    max_examples=25,
    stateful_step_count=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


class TestCacheDictDirectProperties(unittest.TestCase):
    """Property tests for CacheDict operations."""

    @given(
        ops=st.lists(
            st.tuples(
                st.sampled_from(["set", "get", "del", "pop"]),
                st.integers(min_value=0, max_value=25),
                st.text(max_size=20),
            ),
            max_size=60,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_cache_dict_operations_match_dict(
        self, ops: list[tuple[str, int, str]]
    ) -> None:
        """Sequence of set/get/del/pop on CacheDict matches standard dict."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "cd.dict")
            cd = CacheDict(max_size=5, db_file=db_path, lru=True)
            oracle: dict[int, str] = {}
            try:
                for op, key, val in ops:
                    if op == "set":
                        oracle[key] = val
                        cd[key] = val
                    elif op == "get":
                        self.assertEqual(cd.get(key, "MISSING"), oracle.get(key, "MISSING"))
                    elif op == "del":
                        if key in oracle:
                            del oracle[key]
                            del cd[key]
                        else:
                            with self.assertRaises(KeyError):
                                del cd[key]
                    elif op == "pop":
                        if key in oracle:
                            self.assertEqual(cd.pop(key), oracle.pop(key))
                        else:
                            with self.assertRaises(KeyError):
                                cd.pop(key)
                self.assertEqual(len(cd), len(oracle))
                self.assertEqual(dict(cd.items()), oracle)
            finally:
                cd.close()


if __name__ == "__main__":
    unittest.main()
