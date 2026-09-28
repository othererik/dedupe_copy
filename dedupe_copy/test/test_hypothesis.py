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

from dedupe_copy.config import CopyConfig, CopyJob, DeleteJob, WalkConfig
from dedupe_copy.core import (
    _classify_files_for_copy,
    _collect_files_to_delete,
    _populate_collisions_from_manifest,
    _select_files_for_deletion,
    run_dupe_copy,
    verify_manifest_fs,
)
from dedupe_copy.disk_cache_dict import (
    CacheDict,
    DefaultCacheDict,
    PersistentSet,
    _deserialize,
    _serialize,
)
from dedupe_copy.manifest import Manifest
from dedupe_copy.path_rules import build_path_rules, strip_read_path_prefix
from dedupe_copy.threads import (
    CopyThread,
    DeleteThread,
    ReadThread,
    ResultProcessor,
    WalkThread,
    _check_is_ignored,
)
from dedupe_copy.utils import (
    ExtensionMatcher,
    clean_extensions,
    match_extension,
    read_file,
)

pytestmark = pytest.mark.hypothesis

# pylint: disable=protected-access,too-many-lines


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

    @rule(k=keys)
    def pop_item(self, k: str) -> None:
        """Test .pop(k) without default does not trigger default_factory on missing keys."""
        assert self.dcd is not None
        if k in self.oracle:
            assert self.dcd.pop(k) == self.oracle.pop(k)
        else:
            with pytest.raises(KeyError):
                self.dcd.pop(k)

    @rule(k=keys, default=st.integers(min_value=-50, max_value=-1))
    def pop_with_default(self, k: str, default: int) -> None:
        """Test .pop(k, default) returns default on missing keys without mutating cache."""
        assert self.dcd is not None
        assert self.dcd.pop(k, default) == self.oracle.pop(k, default)

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


class TestPathRuleCompositionProperties(unittest.TestCase):
    """Property-based tests for build_path_rules and directory restructuring."""

    segment_strategy = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
        min_size=1,
        max_size=8,
    )

    @given(
        wildcard_rules=st.lists(
            st.sampled_from(["mtime", "extension", "no_change"]),
            min_size=1,
            max_size=3,
        ),
        specific_rules=st.lists(
            st.sampled_from(["mtime", "extension", "no_change"]),
            min_size=0,
            max_size=3,
        ),
        rel_dir_segments=st.lists(segment_strategy, min_size=0, max_size=3),
        file_ext=st.sampled_from(["jpg", "txt", "mp4"]),
        year=st.integers(min_value=1990, max_value=2030),
        month=st.integers(min_value=1, max_value=12),
    )
    @settings(max_examples=120, deadline=None)
    def test_path_rules_containment_and_layer_ordering(
        self,
        wildcard_rules: list[str],
        specific_rules: list[str],
        rel_dir_segments: list[str],
        file_ext: str,
        year: int,
        month: int,
    ) -> None:
        """Composed path rules strictly stay within dest_dir and apply layers in order."""
        rule_pairs = [f"*:{r}" for r in wildcard_rules]
        if specific_rules:
            rule_pairs.extend(f".jpg:{r}" for r in specific_rules)

        parser = build_path_rules(rule_pairs)
        read_root = os.path.abspath("/tmp/hyp_read_root")
        dest_root = os.path.abspath("/tmp/hyp_dest_root")
        source_dirs = (
            os.path.join(read_root, *rel_dir_segments)
            if rel_dir_segments
            else read_root
        )
        src_name = f"sample.{file_ext}"
        mtime_str = f"{year:04d}_{month:02d}"

        dest_file, dest_dir = parser(
            dest_root,
            file_ext,
            mtime_str,
            1024,
            source_dirs=source_dirs,
            src=src_name,
            read_paths=[read_root],
        )

        # 1. Target containment invariant: destination never escapes dest_root
        self.assertEqual(
            os.path.commonpath([dest_root, os.path.abspath(dest_file)]),
            dest_root,
        )
        self.assertEqual(os.path.basename(dest_file), src_name)
        self.assertEqual(os.path.dirname(dest_file), dest_dir)

        # 2. Exact rule precedence and sequential layer construction
        active_rules = (
            specific_rules if (file_ext == "jpg" and specific_rules) else wildcard_rules
        )
        expected_dir = dest_root
        for r in active_rules:
            if r == "mtime":
                expected_dir = os.path.join(expected_dir, mtime_str)
            elif r == "extension":
                expected_dir = os.path.join(expected_dir, file_ext)
            elif r == "no_change" and rel_dir_segments:
                expected_dir = os.path.join(expected_dir, *rel_dir_segments)

        self.assertEqual(os.path.normpath(dest_dir), os.path.normpath(expected_dir))


class TestFilesystemAndWalkThreadingProperties(unittest.TestCase):
    """Property tests for multithreaded WalkThread/ReadThread/ResultProcessor on trees."""

    file_spec_strategy = st.lists(
        st.tuples(
            st.sampled_from(
                [
                    "",
                    "sub_a",
                    "sub_a/deep_1",
                    "sub_b",
                    "sub_b/deep_2/leaf",
                    "ignored_dir",
                    "sub_a/ignored_dir",
                ]
            ),
            st.sampled_from(["f1", "f2", "f3", "f4"]),
            st.sampled_from([".txt", ".jpg", ".bin", ".tmp"]),
            st.sampled_from(
                [
                    b"",
                    b"alpha_payload",
                    b"beta_payload_longer",
                    b"gamma_payload_12345",
                ]
            ),
        ),
        min_size=1,
        max_size=24,
        unique_by=lambda item: (item[0], item[1], item[2]),
    )

    @staticmethod
    def _build_reference_oracle(
        src_root: str,
        created_files: list[tuple[str, bytes]],
        walk_config: WalkConfig,
    ) -> tuple[dict[str, set[str]], set[str]]:
        """Computes the single-threaded expected hashes and read_sources for a tree."""
        oracle_hashes: dict[str, set[str]] = collections.defaultdict(set)
        oracle_sources: set[str] = set()
        ignore_patterns = walk_config.ignore
        ignore_regex = walk_config.ignore_regex
        extensions = walk_config.extensions

        for full_path, _ in created_files:
            rel_to_src = os.path.relpath(full_path, src_root)
            parts = rel_to_src.split(os.sep)
            curr = src_root
            if any(
                _check_is_ignored(
                    curr := os.path.join(curr, part),
                    ignore_patterns,
                    ignore_regex,
                    None,
                )
                for part in parts[:-1]
            ):
                continue
            if _check_is_ignored(full_path, ignore_patterns, ignore_regex, None):
                continue
            if extensions and not match_extension(extensions, full_path):
                continue

            md5, _, _, _ = read_file(full_path)
            norm_p = os.path.normcase(os.path.abspath(full_path))
            oracle_sources.add(norm_p)
            oracle_hashes[md5].add(norm_p)
        return oracle_hashes, oracle_sources

    def _verify_crawl_against_oracle(
        self,
        manifest: Manifest,
        collisions: CacheDict,
        oracle_hashes: dict[str, set[str]],
        oracle_sources: set[str],
        dedupe_empty: bool,
    ) -> None:
        """Asserts that the multithreaded crawl manifest and collisions match the oracle."""
        actual_sources = {
            os.path.normcase(os.path.abspath(p)) for p in manifest.read_sources
        }
        self.assertEqual(actual_sources, oracle_sources)
        self.assertEqual(set(manifest.md5_data.keys()), set(oracle_hashes.keys()))

        for md5, expected_paths in oracle_hashes.items():
            recorded_entries = manifest.md5_data[md5]
            recorded_paths = [
                os.path.normcase(os.path.abspath(entry[0]))
                for entry in recorded_entries
            ]
            self.assertEqual(len(recorded_paths), len(set(recorded_paths)))
            self.assertEqual(set(recorded_paths), expected_paths)

            is_empty_hash = all(entry[1] == 0 for entry in recorded_entries)
            should_collide = len(expected_paths) > 1 and (
                dedupe_empty or not is_empty_hash
            )
            if should_collide:
                self.assertIn(md5, collisions)
                collision_paths = {
                    os.path.normcase(os.path.abspath(entry[0]))
                    for entry in collisions[md5]
                }
                self.assertEqual(collision_paths, expected_paths)
            else:
                self.assertNotIn(md5, collisions)

    @given(
        file_specs=file_spec_strategy,
        walk_thread_count=st.integers(min_value=1, max_value=5),
        read_thread_count=st.integers(min_value=1, max_value=5),
        batch_size=st.integers(min_value=1, max_value=6),
        overlap_read_paths=st.booleans(),
        dedupe_empty=st.booleans(),
        use_ignore=st.booleans(),
        filter_txt_jpg_only=st.booleans(),
    )
    @settings(
        max_examples=35,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_multithreaded_crawl_matches_single_threaded_oracle(
        self,
        file_specs: list[tuple[str, str, str, bytes]],
        walk_thread_count: int,
        read_thread_count: int,
        batch_size: int,
        overlap_read_paths: bool,
        dedupe_empty: bool,
        use_ignore: bool,
        filter_txt_jpg_only: bool,
    ) -> None:
        """Concurrent WalkThread, ReadThread, and ResultProcessor match a single-threaded oracle."""
        with tempfile.TemporaryDirectory(prefix="hyp_tree_") as tmp_dir:
            src_root = os.path.join(tmp_dir, "src")
            os.makedirs(os.path.join(src_root, "empty_branch", "nested"), exist_ok=True)

            created_files: list[tuple[str, bytes]] = []
            for rel_dir, stem, ext, payload in file_specs:
                dir_path = os.path.join(src_root, rel_dir) if rel_dir else src_root
                os.makedirs(dir_path, exist_ok=True)
                full_path = os.path.join(dir_path, f"{stem}{ext}")
                with open(full_path, "wb") as f:
                    f.write(payload)
                created_files.append((full_path, payload))

            walk_config = WalkConfig(
                extensions=[".txt", ".jpg"] if filter_txt_jpg_only else None,
                ignore=["*ignored_dir*", "*.tmp"] if use_ignore else None,
            )
            oracle_hashes, oracle_sources = self._build_reference_oracle(
                src_root, created_files, walk_config
            )

            manifest = Manifest(
                None,
                save_path=os.path.join(tmp_dir, "manifest.db"),
                temp_directory=tmp_dir,
            )
            collisions = CacheDict(
                max_size=6, db_file=os.path.join(tmp_dir, "collisions.db")
            )

            walk_queue: queue.Queue[str] = queue.Queue()
            work_queue: queue.Queue[str] = queue.Queue()
            result_queue: queue.Queue[tuple[str, int, float, str]] = queue.Queue()
            walk_stop, read_stop, result_stop = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            save_event = threading.Event()
            seen_paths: set[str] = set()
            seen_lock = threading.Lock()

            walk_queue.put(src_root)
            sub_a_path = os.path.join(src_root, "sub_a")
            if overlap_read_paths and os.path.isdir(sub_a_path):
                walk_queue.put(sub_a_path)

            orig_batch_size = ResultProcessor.BATCH_SIZE
            ResultProcessor.BATCH_SIZE = batch_size
            try:
                result_proc = ResultProcessor(
                    result_stop,
                    result_queue,
                    collisions,
                    manifest,
                    dedupe_empty=dedupe_empty,
                    save_event=save_event,
                )
                result_proc.start()

                walkers = [
                    WalkThread(
                        walk_queue,
                        walk_stop,
                        walk_config=walk_config,
                        work_queue=work_queue,
                        already_processed=manifest.read_sources,
                        save_event=save_event,
                        seen_paths=seen_paths,
                        seen_lock=seen_lock,
                    )
                    for _ in range(walk_thread_count)
                ]
                readers = [
                    ReadThread(
                        work_queue,
                        result_queue,
                        read_stop,
                        walk_config=walk_config,
                        save_event=save_event,
                    )
                    for _ in range(read_thread_count)
                ]
                for worker in [*walkers, *readers]:
                    worker.start()

                walk_queue.join()
                walk_stop.set()
                for w in walkers:
                    w.join(timeout=5.0)
                    self.assertFalse(w.is_alive())

                work_queue.join()
                read_stop.set()
                for r in readers:
                    r.join(timeout=5.0)
                    self.assertFalse(r.is_alive())

                result_queue.join()
                result_stop.set()
                result_proc.join(timeout=5.0)
                self.assertFalse(result_proc.is_alive())

                self._verify_crawl_against_oracle(
                    manifest, collisions, oracle_hashes, oracle_sources, dedupe_empty
                )
            finally:
                ResultProcessor.BATCH_SIZE = orig_batch_size
                collisions.close()
                manifest.close()


class TestCopyAndDeleteThreadingProperties(unittest.TestCase):
    """Property tests for concurrent CopyThread collision resolution and DeleteThread safety."""

    def _verify_copy_conservation(
        self,
        source_payloads: dict[str, bytes],
        deleted_records: dict[str, str],
        copy_config: CopyConfig,
    ) -> None:
        """Verifies no source file is lost and rename_on_collision copies all sources."""
        for spath, payload in source_payloads.items():
            src_exists = os.path.exists(spath)
            if copy_config.delete_on_copy and spath in deleted_records:
                self.assertFalse(src_exists)
                dest_written = deleted_records[spath]
                self.assertTrue(os.path.exists(dest_written))
                with open(dest_written, "rb") as f:
                    self.assertEqual(f.read(), payload)
            else:
                self.assertTrue(
                    src_exists,
                    f"Skipped source file {spath} was deleted on collision!",
                )

        if copy_config.rename_on_collision:
            self.assertEqual(
                len(copy_config.claimed_destinations), len(source_payloads)
            )
            if copy_config.delete_on_copy:
                self.assertEqual(len(deleted_records), len(source_payloads))

    @given(
        specs=st.lists(
            st.tuples(
                st.sampled_from(["dir_0", "dir_1", "dir_2", "dir_3"]),
                st.sampled_from(["item_a.txt", "item_b.txt", "item_c.jpg"]),
                st.binary(min_size=1, max_size=64),
            ),
            min_size=1,
            max_size=16,
            unique_by=lambda x: (x[0], x[1]),
        ),
        copy_thread_count=st.integers(min_value=1, max_value=5),
        rename_on_collision=st.booleans(),
        delete_on_copy=st.booleans(),
        preexisting_conflict=st.booleans(),
    )
    @settings(
        max_examples=35,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_concurrent_copy_threads_collision_and_delete_on_copy_safety(
        self,
        specs: list[tuple[str, str, bytes]],
        copy_thread_count: int,
        rename_on_collision: bool,
        delete_on_copy: bool,
        preexisting_conflict: bool,
    ) -> None:
        """Concurrent CopyThreads never lose data on collision or delete skipped source files."""
        with tempfile.TemporaryDirectory(prefix="hyp_copy_") as tmp_dir:
            src_root = os.path.join(tmp_dir, "src")
            dest_root = os.path.join(tmp_dir, "dest")
            os.makedirs(src_root, exist_ok=True)
            os.makedirs(dest_root, exist_ok=True)

            preexisting_dest_path = os.path.join(dest_root, "txt", "item_a.txt")
            preexisting_bytes = b"PREEXISTING_PROTECTED_CONTENT"
            if preexisting_conflict:
                os.makedirs(os.path.dirname(preexisting_dest_path), exist_ok=True)
                with open(preexisting_dest_path, "wb") as f:
                    f.write(preexisting_bytes)

            work_queue: queue.Queue[tuple[str, str, int]] = queue.Queue()
            deleted_queue: queue.Queue[tuple[str, str]] = queue.Queue()
            stop_event = threading.Event()

            source_payloads: dict[str, bytes] = {}
            for subdir, filename, payload in specs:
                sdir = os.path.join(src_root, subdir)
                os.makedirs(sdir, exist_ok=True)
                spath = os.path.join(sdir, filename)
                with open(spath, "wb") as f:
                    f.write(payload)
                source_payloads[spath] = payload
                work_queue.put((spath, "2026_09", len(payload)))

            copy_config = CopyConfig(
                target_path=dest_root,
                read_paths=[src_root],
                extensions=None,
                path_rules=build_path_rules(["*:extension"]),
                preserve_stat=False,
                delete_on_copy=delete_on_copy,
                dry_run=False,
                rename_on_collision=rename_on_collision,
            )

            workers = [
                CopyThread(
                    work_queue,
                    stop_event,
                    copy_config=copy_config,
                    deleted_queue=deleted_queue,
                )
                for _ in range(copy_thread_count)
            ]
            for w in workers:
                w.start()

            work_queue.join()
            stop_event.set()
            for w in workers:
                w.join(timeout=5.0)
                self.assertFalse(w.is_alive())

            if preexisting_conflict:
                self.assertTrue(os.path.exists(preexisting_dest_path))
                with open(preexisting_dest_path, "rb") as f:
                    self.assertEqual(f.read(), preexisting_bytes)

            deleted_records: dict[str, str] = {}
            while not deleted_queue.empty():
                s_del, d_del = deleted_queue.get_nowait()
                deleted_records[s_del] = d_del

            self._verify_copy_conservation(
                source_payloads, deleted_records, copy_config
            )

    @given(
        existing_count=st.integers(min_value=0, max_value=15),
        missing_count=st.integers(min_value=0, max_value=8),
        thread_count=st.integers(min_value=1, max_value=5),
        dry_run=st.booleans(),
    )
    @settings(max_examples=30, deadline=None)
    def test_concurrent_delete_threads_exact_accounting(
        self,
        existing_count: int,
        missing_count: int,
        thread_count: int,
        dry_run: bool,
    ) -> None:
        """Concurrent DeleteThreads remove existing files and handle missing files safely."""
        with tempfile.TemporaryDirectory(prefix="hyp_del_thr_") as tmp_dir:
            work_queue: queue.Queue[str] = queue.Queue()
            deleted_queue: queue.Queue[str] = queue.Queue()
            progress_queue: queue.PriorityQueue[Any] = queue.PriorityQueue()
            stop_event = threading.Event()

            existing_paths: set[str] = set()
            for i in range(existing_count):
                p = os.path.join(tmp_dir, f"exists_{i}.dat")
                with open(p, "wb") as f:
                    f.write(b"x")
                existing_paths.add(p)
                work_queue.put(p)

            for i in range(missing_count):
                work_queue.put(os.path.join(tmp_dir, f"missing_{i}.dat"))

            workers = [
                DeleteThread(
                    work_queue,
                    stop_event,
                    progress_queue=progress_queue,
                    deleted_queue=deleted_queue,
                    dry_run=dry_run,
                )
                for _ in range(thread_count)
            ]
            for w in workers:
                w.start()

            work_queue.join()
            stop_event.set()
            for w in workers:
                w.join(timeout=5.0)
                self.assertFalse(w.is_alive())

            recorded_deleted: set[str] = set()
            while not deleted_queue.empty():
                recorded_deleted.add(deleted_queue.get_nowait())

            if dry_run:
                self.assertEqual(len(recorded_deleted), 0)
                for p in existing_paths:
                    self.assertTrue(os.path.exists(p))
            else:
                self.assertEqual(recorded_deleted, existing_paths)
                for p in existing_paths:
                    self.assertFalse(os.path.exists(p))


class ManifestStateMachine(RuleBasedStateMachine):
    """Stateful property test for Manifest lifecycle (add, remove, move, convert, save/load)."""

    def __init__(self) -> None:
        super().__init__()
        self.temp_dir = tempfile.mkdtemp(prefix="hyp_manifest_sm_")
        self.manifest_path = os.path.join(self.temp_dir, "state_manifest.db")
        # Oracle maps path -> (md5, size, mtime)
        self.oracle_files: dict[str, tuple[str, int, float]] = {}
        self.manifest: Optional[Manifest] = None
        self.prefix = "/vol_a"

    @initialize()
    def init_manifest(self) -> None:
        """Create a fresh Manifest instance."""
        self.manifest = Manifest(
            None, save_path=self.manifest_path, temp_directory=self.temp_dir
        )

    @rule(
        md5=st.sampled_from(["h1", "h2", "h3", "h4"]),
        rel_name=st.sampled_from(["f1.txt", "f2.txt", "sub/f3.jpg", "sub/f4.bin"]),
        size=st.integers(min_value=1, max_value=5000),
        mtime=st.floats(
            min_value=1000.0, max_value=2000.0, allow_nan=False, allow_infinity=False
        ),
    )
    def add_or_update_file(
        self, md5: str, rel_name: str, size: int, mtime: float
    ) -> None:
        """Insert or update a file entry in the manifest."""
        assert self.manifest is not None
        full_path = f"{self.prefix}/{rel_name}"
        # If full_path previously belonged to another hash, remove it first
        if full_path in self.oracle_files:
            self.manifest.remove_files([full_path])
        self.oracle_files[full_path] = (md5, size, mtime)
        current = list(self.manifest.md5_data.get(md5) or [])
        current = [entry for entry in current if entry[0] != full_path]
        current.append((full_path, size, mtime))
        self.manifest.md5_data[md5] = current
        self.manifest.read_sources.add(full_path)

    @rule(
        rel_names=st.lists(
            st.sampled_from(["f1.txt", "f2.txt", "sub/f3.jpg", "sub/f4.bin"]),
            min_size=1,
            max_size=3,
            unique=True,
        )
    )
    def remove_some_files(self, rel_names: list[str]) -> None:
        """Test Manifest.remove_files removes file entries and prunes empty hashes."""
        assert self.manifest is not None
        to_remove = [f"{self.prefix}/{name}" for name in rel_names]
        for p in to_remove:
            self.oracle_files.pop(p, None)
        self.manifest.remove_files(to_remove)

    @rule(
        src_rel=st.sampled_from(["f1.txt", "f2.txt", "sub/f3.jpg"]),
        dst_rel=st.sampled_from(["moved_1.txt", "moved_2.txt", "sub/moved_3.jpg"]),
    )
    def move_file_path(self, src_rel: str, dst_rel: str) -> None:
        """Test Manifest.update_paths when moving an existing file to an unoccupied path."""
        assert self.manifest is not None
        src_p = f"{self.prefix}/{src_rel}"
        dst_p = f"{self.prefix}/{dst_rel}"
        if src_p in self.oracle_files and dst_p not in self.oracle_files:
            self.oracle_files[dst_p] = self.oracle_files.pop(src_p)
            self.manifest.update_paths([(src_p, dst_p)])

    @rule(new_prefix=st.sampled_from(["/vol_a", "/vol_b", "/mnt/archive"]))
    def convert_prefix(self, new_prefix: str) -> None:
        """Test Manifest.convert_manifest_paths across md5_data and read_sources."""
        assert self.manifest is not None
        if new_prefix == self.prefix:
            return
        old_prefix = self.prefix
        updated_oracle: dict[str, tuple[str, int, float]] = {}
        for path, meta in self.oracle_files.items():
            updated_oracle[path.replace(old_prefix, new_prefix, 1)] = meta
        self.oracle_files = updated_oracle
        self.manifest.convert_manifest_paths(
            old_prefix, new_prefix, temp_directory=self.temp_dir
        )
        self.prefix = new_prefix

    @rule(rebuild=st.booleans())
    def save_and_reload(self, rebuild: bool) -> None:
        """Test Manifest.save() and Manifest.load()."""
        assert self.manifest is not None
        self.manifest.save(rebuild_sources=rebuild)
        self.manifest.load()

    @invariant()
    def check_manifest_invariants(self) -> None:
        """Verify md5_data, read_sources, and collision detection match oracle."""
        if self.manifest is None:
            return
        expected_by_hash: dict[str, dict[str, tuple[int, float]]] = (
            collections.defaultdict(dict)
        )
        for path, (md5, size, mtime) in self.oracle_files.items():
            expected_by_hash[md5][path] = (size, mtime)

        assert set(self.manifest.md5_data.keys()) == set(expected_by_hash.keys())
        assert set(self.manifest.read_sources) == set(self.oracle_files.keys())

        for md5, path_map in expected_by_hash.items():
            actual_entries = self.manifest.md5_data[md5]
            actual_map = {entry[0]: (entry[1], entry[2]) for entry in actual_entries}
            assert len(actual_entries) == len(actual_map)
            assert actual_map == path_map

    def teardown(self) -> None:
        """Close manifest and remove temporary directory."""
        if self.manifest is not None:
            self.manifest.close()
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


TestManifestStateMachine = ManifestStateMachine.TestCase
TestManifestStateMachine.settings = settings(
    max_examples=25,
    stateful_step_count=18,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


class TestManifestCombineAndCopyClassificationProperties(unittest.TestCase):
    """Property tests for manifest combining and _classify_files_for_copy invariants."""

    @given(
        entries_m1=st.lists(
            st.tuples(
                st.sampled_from(["h1", "h2", "h3"]),
                st.sampled_from(["a.txt", "b.txt", "c.jpg"]),
                st.integers(min_value=10, max_value=500),
                st.floats(
                    min_value=1000.0,
                    max_value=2000.0,
                    allow_nan=False,
                    allow_infinity=False,
                ),
            ),
            min_size=1,
            max_size=8,
            unique_by=lambda x: x[1],
        ),
        mtime_delta=st.floats(
            min_value=0.0, max_value=50.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(
        max_examples=35,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_combine_manifests_physical_path_dedup_and_no_self_collision(
        self,
        entries_m1: list[tuple[str, str, int, float]],
        mtime_delta: float,
    ) -> None:
        """Combining overlapping manifests deduplicates physical paths without self-collisions."""
        with tempfile.TemporaryDirectory(prefix="hyp_comb_") as tmp_dir:
            m1_path = os.path.join(tmp_dir, "m1.db")
            m2_path = os.path.join(tmp_dir, "m2.db")
            m1 = Manifest(None, save_path=m1_path, temp_directory=tmp_dir)
            m2 = Manifest(None, save_path=m2_path, temp_directory=tmp_dir)

            try:
                for md5, name, size, mtime in entries_m1:
                    p1 = os.path.join(tmp_dir, "data", name)
                    # Second manifest uses redundant './' segment and updated mtime
                    p2 = os.path.join(tmp_dir, "data", ".", name)
                    m1.md5_data[md5].append((p1, size, mtime))
                    m1.read_sources.add(p1)
                    m2.md5_data[md5].append((p2, size, mtime + mtime_delta))
                    m2.read_sources.add(p2)

                m1.save(rebuild_sources=False)
                m2.save(rebuild_sources=False)
            finally:
                m1.close()
                m2.close()

            combined = Manifest(
                [m1_path, m2_path],
                save_path=os.path.join(tmp_dir, "combined.db"),
                temp_directory=tmp_dir,
            )
            try:
                expected_by_hash: dict[str, set[str]] = collections.defaultdict(set)
                for md5, name, _, _ in entries_m1:
                    norm_p = os.path.normcase(
                        os.path.abspath(os.path.join(tmp_dir, "data", name))
                    )
                    expected_by_hash[md5].add(norm_p)

                for md5, expected_paths in expected_by_hash.items():
                    actual_paths = [
                        os.path.normcase(os.path.abspath(entry[0]))
                        for entry in combined.md5_data[md5]
                    ]
                    self.assertEqual(len(actual_paths), len(set(actual_paths)))
                    self.assertEqual(set(actual_paths), expected_paths)

                collisions: dict[str, Any] = {}
                _populate_collisions_from_manifest(
                    combined, collisions, dedupe_empty=True
                )
                for md5, exp_paths in expected_by_hash.items():
                    if len(exp_paths) > 1:
                        self.assertIn(md5, collisions)
                    else:
                        self.assertNotIn(md5, collisions)
            finally:
                combined.close()

    @given(
        items=st.lists(
            st.tuples(
                st.sampled_from(["h1", "h2", "h3", "h_empty"]),
                st.sampled_from(
                    [
                        "/src/keep_1.jpg",
                        "/src/keep_2.jpg",
                        "/src/doc_1.txt",
                        "/src/doc_2.txt",
                        "/src/ignored_dir/secret.jpg",
                    ]
                ),
                st.booleans(),
            ),
            min_size=1,
            max_size=15,
        ),
        filter_jpg_only=st.booleans(),
        use_ignore=st.booleans(),
        delete_on_copy=st.booleans(),
        dedupe_empty=st.booleans(),
        preseed_h1_in_compare=st.booleans(),
    )
    @settings(max_examples=100, deadline=None)
    def test_classify_files_for_copy_immunity_and_conservation_laws(
        self,
        items: list[tuple[str, str, bool]],
        filter_jpg_only: bool,
        use_ignore: bool,
        delete_on_copy: bool,
        dedupe_empty: bool,
        preseed_h1_in_compare: bool,
    ) -> None:
        """Ignored or extension-excluded files are never copied or queued for deletion."""
        all_data: dict[str, list[tuple[str, int, float]]] = collections.defaultdict(
            list
        )
        for idx, (md5, path, use_dot_segment) in enumerate(items):
            size = 0 if md5 == "h_empty" else 128
            raw_path = path.replace("/src/", "/src/./") if use_dot_segment else path
            all_data[md5].append((raw_path, size, 1700000000.0 + idx))

        extensions = [".jpg"] if filter_jpg_only else None
        ignore_patterns = ["*ignored_dir*"] if use_ignore else None
        ignore_regex = (
            WalkConfig(ignore=ignore_patterns).ignore_regex if use_ignore else None
        )

        copy_config = CopyConfig(
            target_path="/dest",
            read_paths=["/src"],
            extensions=extensions,
            path_rules=None,
            preserve_stat=False,
            delete_on_copy=delete_on_copy,
            dry_run=False,
        )
        copy_job = CopyJob(
            copy_config=copy_config,
            ignore=ignore_patterns,
            no_copy=None,
            dedupe_empty=dedupe_empty,
            copy_threads=1,
            delete_on_copy=delete_on_copy,
            dry_run=False,
        )

        hashes_to_skip = {"h1"} if preseed_h1_in_compare else set()
        delete_only_queue: queue.Queue[str] = queue.Queue()

        files_to_copy = _classify_files_for_copy(
            all_data,
            hashes_to_skip,
            ignore_regex,
            copy_job,
            delete_only_queue,
            None,
        )

        copied_paths = [entry[0] for entry in files_to_copy]
        deleted_paths: list[str] = []
        while not delete_only_queue.empty():
            deleted_paths.append(delete_only_queue.get_nowait())

        norm_copied = {os.path.normcase(os.path.abspath(p)) for p in copied_paths}
        norm_deleted = {os.path.normcase(os.path.abspath(p)) for p in deleted_paths}

        # 1. Disjointness: no physical file can be both copied and delete-only queued
        self.assertTrue(norm_copied.isdisjoint(norm_deleted))

        # 2. Immunity: no ignored or extension-excluded file is copied or deleted
        for p in [*copied_paths, *deleted_paths]:
            if use_ignore:
                self.assertNotIn("ignored_dir", p)
            if filter_jpg_only:
                self.assertTrue(p.endswith(".jpg"))


class TestEndToEndWorkflowProperties(unittest.TestCase):
    """End-to-end property tests for run_dupe_copy conservation and manifest verification."""

    @given(
        file_specs=st.lists(
            st.tuples(
                st.sampled_from(["a", "b", "b/nested"]),
                st.sampled_from(["f1.txt", "f2.txt", "f3.jpg"]),
                st.sampled_from([b"payload_1", b"payload_2", b"payload_3"]),
            ),
            min_size=1,
            max_size=12,
            unique_by=lambda x: (x[0], x[1]),
        ),
        mode=st.sampled_from(["copy", "move", "delete_dupes"]),
        rename_on_collision=st.booleans(),
    )
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_run_dupe_copy_conserves_unique_payloads_and_verifies_manifest(
        self,
        file_specs: list[tuple[str, str, bytes]],
        mode: str,
        rename_on_collision: bool,
    ) -> None:
        """run_dupe_copy preserves all unique file contents and writes a verifiable manifest."""
        with tempfile.TemporaryDirectory(prefix="hyp_e2e_") as tmp_dir:
            src_root = os.path.join(tmp_dir, "src")
            dest_root = os.path.join(tmp_dir, "dest")
            manifest_out = os.path.join(tmp_dir, "out_manifest.db")
            os.makedirs(src_root, exist_ok=True)

            expected_payloads: set[bytes] = set()
            for subdir, fname, payload in file_specs:
                dpath = os.path.join(src_root, subdir)
                os.makedirs(dpath, exist_ok=True)
                with open(os.path.join(dpath, fname), "wb") as f:
                    f.write(payload)
                expected_payloads.add(payload)

            copy_to_path = dest_root if mode in ("copy", "move") else None
            delete_on_copy = mode == "move"
            delete_duplicates = mode == "delete_dupes"

            rc = run_dupe_copy(
                read_from_path=[src_root],
                manifest_out_path=manifest_out,
                copy_to_path=copy_to_path,
                path_rules=["*:extension"] if copy_to_path else None,
                walk_threads=2,
                read_threads=2,
                copy_threads=2,
                delete_duplicates=delete_duplicates,
                delete_on_copy=delete_on_copy,
                rename_on_collision=rename_on_collision,
                use_ui=False,
            )
            self.assertEqual(rc, 0)

            # 1. Every unique payload must still exist somewhere in src_root or dest_root
            surviving_payloads: set[bytes] = set()
            for scan_root in (src_root, dest_root):
                if not os.path.exists(scan_root):
                    continue
                for root, _, files in os.walk(scan_root):
                    for file_name in files:
                        with open(os.path.join(root, file_name), "rb") as fh:
                            surviving_payloads.add(fh.read())
            self.assertEqual(surviving_payloads, expected_payloads)

            # 2. Saved output manifest must pass verify_manifest_fs
            loaded_manifest = Manifest(manifest_out, temp_directory=tmp_dir)
            try:
                self.assertTrue(verify_manifest_fs(loaded_manifest, ui=None))
            finally:
                loaded_manifest.close()


if __name__ == "__main__":
    unittest.main()
