"""Regression tests for v1.2.3 correctness, data safety, and performance fixes."""

import os
import queue
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch

from dedupe_copy.bin.dedupecopy_cli import run_cli
from dedupe_copy.config import CopyConfig, DeleteJob, WalkConfig
from dedupe_copy.core import delete_files, run_dupe_copy
from dedupe_copy.disk_cache_dict import (
    CacheDict,
    PersistentSet,
    SqliteBackend,
    SqliteSetBackend,
)
from dedupe_copy.manifest import Manifest, _stage_sqlite_file
from dedupe_copy.path_rules import strip_read_path_prefix
from dedupe_copy.threads import (
    CopyThread,
    DistributeWorkConfig,
    WalkThread,
    _copy_file,
    _is_file_processing_required,
    _mark_path_seen,
    distribute_work,
)
from dedupe_copy.utils import ExtensionMatcher, MAX_TARGET_QUEUE_SIZE, _throttle_puts


class TestV123Correctness(unittest.TestCase):
    """Comprehensive tests for v1.2.3 fixes."""

    def setUp(self):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        self.temp_dir = tempfile.mkdtemp(prefix="dedupe_v123_test_")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_file(self, rel_path: str, content: bytes = b"hello") -> str:
        full_path = os.path.join(self.temp_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)
        return full_path

    def test_failed_copy_does_not_delete_source_and_cleans_partial_dest(self):
        """If copying fails mid-flight, source must never be deleted and partial dest is removed."""
        src_file = self._create_file("src/important.txt", b"precious data" * 100)
        dest_dir = os.path.join(self.temp_dir, "dest")
        manifest_out = os.path.join(self.temp_dir, "out.db")

        def failing_copyfile(_src, dst, *_args, **_kwargs):
            # Simulate partial write before disk full error
            with open(dst, "wb") as f:
                f.write(b"partial")
            raise OSError("Simulated disk full error")

        with patch("dedupe_copy.threads.shutil.copyfile", side_effect=failing_copyfile):
            run_dupe_copy(
                read_from_path=[os.path.join(self.temp_dir, "src")],
                copy_to_path=dest_dir,
                manifest_out_path=manifest_out,
                delete_on_copy=True,
            )

        # Source file MUST still exist and be intact
        self.assertTrue(os.path.exists(src_file))
        with open(src_file, "rb") as f:
            self.assertEqual(f.read(), b"precious data" * 100)

        # Partial destination file MUST have been cleaned up
        expected_dest = os.path.join(dest_dir, "important.txt")
        self.assertFalse(os.path.exists(expected_dest))

        # Manifest must still reference the source file, not the failed destination
        m = Manifest(
            manifest_out, temp_directory=os.path.join(self.temp_dir, "verify_tmp")
        )
        try:
            all_paths = [
                entry[0] for file_list in m.md5_data.values() for entry in file_list
            ]
            self.assertIn(src_file, all_paths)
            self.assertNotIn(expected_dest, all_paths)
        finally:
            m.close()

    def test_same_file_copy_does_not_delete_source_on_delete_on_copy(self):
        """Copying where src and dest resolve to the same file must never delete the file."""
        src_dir = os.path.join(self.temp_dir, "same_dir")
        src_file = self._create_file("same_dir/data.txt", b"keep me safe")
        manifest_out = os.path.join(self.temp_dir, "same_out.db")

        run_dupe_copy(
            read_from_path=[src_dir],
            copy_to_path=src_dir,
            manifest_out_path=manifest_out,
            delete_on_copy=True,
        )

        self.assertTrue(os.path.exists(src_file))
        with open(src_file, "rb") as f:
            self.assertEqual(f.read(), b"keep me safe")

    def test_destination_collision_default_skips_and_preserves_source(self):
        """Different-content files mapping to the same dest path do not overwrite by default."""
        src1_file = self._create_file("src1/photo.jpg", b"Camera A unique photo")
        src2_file = self._create_file("src2/photo.jpg", b"Camera B different photo")
        dest_dir = os.path.join(self.temp_dir, "dest")
        manifest_out = os.path.join(self.temp_dir, "collision_default.db")

        run_dupe_copy(
            read_from_path=[
                os.path.join(self.temp_dir, "src1"),
                os.path.join(self.temp_dir, "src2"),
            ],
            copy_to_path=dest_dir,
            manifest_out_path=manifest_out,
            delete_on_copy=True,
            rename_on_collision=False,
            copy_threads=2,
        )

        dest_file = os.path.join(dest_dir, "photo.jpg")
        self.assertTrue(os.path.exists(dest_file))
        with open(dest_file, "rb") as f:
            dest_content = f.read()

        # Exactly one of the two sources was copied and deleted; the other was skipped and kept!
        remaining_sources = [p for p in (src1_file, src2_file) if os.path.exists(p)]
        self.assertEqual(len(remaining_sources), 1)
        with open(remaining_sources[0], "rb") as f:
            remaining_content = f.read()

        self.assertEqual(
            {dest_content, remaining_content},
            {b"Camera A unique photo", b"Camera B different photo"},
        )

    def test_destination_collision_preexisting_different_file_not_overwritten(self):
        """A pre-existing destination file with different content is not overwritten."""
        src_file = self._create_file("src/report.txt", b"New report content")
        dest_file = self._create_file("dest/report.txt", b"Existing different report")
        manifest_out = os.path.join(self.temp_dir, "preexisting.db")

        run_dupe_copy(
            read_from_path=[os.path.join(self.temp_dir, "src")],
            copy_to_path=os.path.join(self.temp_dir, "dest"),
            manifest_out_path=manifest_out,
            delete_on_copy=True,
            rename_on_collision=False,
        )

        # Pre-existing dest file is untouched
        with open(dest_file, "rb") as f:
            self.assertEqual(f.read(), b"Existing different report")
        # Source file was NOT deleted because copy was skipped due to collision
        self.assertTrue(os.path.exists(src_file))
        with open(src_file, "rb") as f:
            self.assertEqual(f.read(), b"New report content")

    def test_destination_collision_rename_on_collision_flag(self):
        """With rename_on_collision=True, colliding files are renamed to file_1.ext, file_2.ext."""
        f1 = self._create_file("src1/photo.jpg", b"Content One")
        f2 = self._create_file("src2/photo.jpg", b"Content Two")
        f3 = self._create_file("src3/photo.jpg", b"Content Three")
        dest_dir = os.path.join(self.temp_dir, "dest_renamed")
        manifest_out = os.path.join(self.temp_dir, "renamed.db")

        run_dupe_copy(
            read_from_path=[
                os.path.join(self.temp_dir, "src1"),
                os.path.join(self.temp_dir, "src2"),
                os.path.join(self.temp_dir, "src3"),
            ],
            copy_to_path=dest_dir,
            manifest_out_path=manifest_out,
            delete_on_copy=True,
            rename_on_collision=True,
            copy_threads=2,
        )

        # All 3 source files should have been moved to dest under disambiguated names
        self.assertFalse(os.path.exists(f1))
        self.assertFalse(os.path.exists(f2))
        self.assertFalse(os.path.exists(f3))

        expected_dest_files = [
            os.path.join(dest_dir, "photo.jpg"),
            os.path.join(dest_dir, "photo_1.jpg"),
            os.path.join(dest_dir, "photo_2.jpg"),
        ]
        actual_contents = set()
        for dp in expected_dest_files:
            self.assertTrue(os.path.exists(dp), f"Expected {dp} to exist")
            with open(dp, "rb") as f:
                actual_contents.add(f.read())

        self.assertEqual(
            actual_contents, {b"Content One", b"Content Two", b"Content Three"}
        )

        # Verify manifest records the 3 destination paths
        m = Manifest(
            manifest_out, temp_directory=os.path.join(self.temp_dir, "verify_ren")
        )
        try:
            manifest_paths = {
                entry[0] for file_list in m.md5_data.values() for entry in file_list
            }
            self.assertEqual(manifest_paths, set(expected_dest_files))
        finally:
            m.close()

    def test_overlapping_read_paths_never_delete_sole_file(self):
        """Overlapping or duplicate read_from_path must never treat a single file as a duplicate."""
        sole_file = self._create_file(
            "root/sub/unique.txt", b"only copy in the universe"
        )
        root_dir = os.path.join(self.temp_dir, "root")
        sub_dir = os.path.join(self.temp_dir, "root", "sub")
        manifest_out = os.path.join(self.temp_dir, "overlap.db")

        run_dupe_copy(
            read_from_path=[root_dir, sub_dir, root_dir],
            delete_duplicates=True,
            manifest_out_path=manifest_out,
        )

        self.assertTrue(
            os.path.exists(sole_file), "Sole physical file must not be deleted!"
        )

    def test_delete_files_deduplicates_identical_paths_in_collision_list(self):
        """delete_files() must deduplicate equivalent paths before deleting."""
        sole_file = self._create_file("dir/sole.txt", b"important")
        non_norm_path = os.path.join(self.temp_dir, "dir", ".", "sole.txt")

        collisions = {
            "deadbeef": [
                (sole_file, 9, 100.0),
                (non_norm_path, 9, 100.0),
            ]
        }
        deleted = delete_files(
            collisions,
            None,
            delete_job=DeleteJob(dry_run=False),
        )
        self.assertEqual(deleted, [])
        self.assertTrue(os.path.exists(sole_file))

    def test_strip_read_path_prefix_strict_component_boundaries(self):
        """Sibling directories sharing a prefix must not corrupt relative paths."""
        src_a = os.path.join(self.temp_dir, "photos")
        src_b = os.path.join(self.temp_dir, "photos_backup")
        self._create_file("photos/a.jpg", b"img_a")
        self._create_file("photos_backup/2024/b.jpg", b"img_b")
        dest_dir = os.path.join(self.temp_dir, "out_copy")

        run_dupe_copy(
            read_from_path=[src_a, src_b],
            copy_to_path=dest_dir,
        )

        self.assertTrue(os.path.exists(os.path.join(dest_dir, "a.jpg")))
        self.assertTrue(os.path.exists(os.path.join(dest_dir, "2024", "b.jpg")))
        self.assertFalse(os.path.exists(os.path.join(dest_dir, "_backup")))

        # Also check nested read_paths with strip_read_path_prefix directly
        rel, matched = strip_read_path_prefix(
            "/data/archive/2024/pic.jpg",
            ["/data", "/data/archive"],
        )
        self.assertTrue(matched)
        self.assertEqual(rel, "2024/pic.jpg")

    def test_cross_process_pythonhashseed_sqlite_persistence(self):
        """Manifests and SQLite backends must work across different PYTHONHASHSEEDs."""
        db_path = os.path.join(self.temp_dir, "hashseed_dict.db")
        set_path = os.path.join(self.temp_dir, "hashseed_set.db")
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        writer_code = f"""
import sys
sys.path.insert(0, {repo_root!r})
from dedupe_copy.disk_cache_dict import DefaultCacheDict, PersistentSet
d = DefaultCacheDict(list, db_file={db_path!r}, max_size=2)
for i in range(10):
    d[f"hash_key_{{i}}"] = [(f"/path/{{i}}", i, 1.0)]
d.save()
d.close()

s = PersistentSet(db_file={set_path!r}, max_size=2)
for i in range(10):
    s.add(f"/path/{{i}}")
s.save()
s.close()
"""
        reader_code = f"""
import sys
sys.path.insert(0, {repo_root!r})
from dedupe_copy.disk_cache_dict import DefaultCacheDict, PersistentSet
d = DefaultCacheDict(list, db_file={db_path!r}, max_size=2)
for i in range(10):
    k = f"hash_key_{{i}}"
    assert k in d, f"Missing key {{k}} in DefaultCacheDict"
    assert d[k] == [(f"/path/{{i}}", i, 1.0)], f"Unexpected value for {{k}}: {{d[k]}}"
d.close()

s = PersistentSet(db_file={set_path!r}, max_size=2)
for i in range(10):
    p = f"/path/{{i}}"
    assert p in s, f"Missing value {{p}} in PersistentSet"
s.close()
"""
        env1 = os.environ.copy()
        env1["PYTHONHASHSEED"] = "101"
        res1 = subprocess.run(
            [sys.executable, "-c", writer_code],
            env=env1,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res1.returncode, 0, f"Writer failed: {res1.stderr}")

        env2 = os.environ.copy()
        env2["PYTHONHASHSEED"] = "999"
        res2 = subprocess.run(
            [sys.executable, "-c", reader_code],
            env=env2,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res2.returncode, 0, f"Reader failed: {res2.stderr}")

    def test_cachedict_disjointness_and_backend_invariants(self):
        """Verify CacheDict cache/db disjointness, KeyError on delete, and copy isolation."""
        # pylint: disable=protected-access
        d = CacheDict(max_size=3, db_file=os.path.join(self.temp_dir, "orig.db"))
        try:
            # Populate enough items to force eviction into SQLite
            for i in range(6):
                d[f"k{i}"] = i
            self.assertEqual(len(d), 6)

            # Find keys currently in SQLite (not in _cache) and one in _cache
            db_keys = [k for k in [f"k{i}" for i in range(6)] if k not in d._cache]
            cache_key = next(iter(d._cache))
            self.assertTrue(len(db_keys) >= 2)
            target_key = db_keys[0]
            remaining_key = db_keys[1]

            # Free 1 slot in _cache so update_batch takes the _update_cache_batch path
            del d[cache_key]
            self.assertEqual(len(d), 5)

            # Updating via update_batch where target_key goes into _cache must remove it from _db
            d.update_batch({target_key: 999})
            self.assertIn(target_key, d._cache)
            self.assertNotIn(target_key, d._db)
            self.assertEqual(d[target_key], 999)
            self.assertEqual(len(d), 5)

            # Deleting target_key must completely remove it from d
            del d[target_key]
            self.assertNotIn(target_key, d)
            self.assertEqual(len(d), 4)

            # Deleting a missing key from SqliteBackend must raise KeyError
            with self.assertRaises(KeyError):
                del d._db["nonexistent_key_xyz"]

            # Test __setitem__ with _evict_lock_held writes to _db without duplicating into _cache
            d._evict_lock_held = True
            try:
                d["reentrant_key"] = "val"
            finally:
                d._evict_lock_held = False
            self.assertNotIn("reentrant_key", d._cache)
            self.assertIn("reentrant_key", d._db)
            self.assertEqual(len(d), 5)

            # Test copy() and fromkeys() isolation
            d_copy = d.copy(db_file=os.path.join(self.temp_dir, "copy.db"))
            d_fk = d.fromkeys(
                ["fk1", "fk2"], 42, db_file=os.path.join(self.temp_dir, "fk.db")
            )
            try:
                self.assertIsNot(d_copy._db, d._db)
                d_copy[remaining_key] = 12345
                self.assertNotEqual(d[remaining_key], 12345)
                self.assertEqual(d_fk["fk1"], 42)
            finally:
                d_copy.close()
                d_fk.close()
        finally:
            d.close()

    def test_sqlite_backends_and_persistent_set_edge_cases(self):
        """Cover SqliteBackend, SqliteSetBackend, and PersistentSet fast-paths and edge cases."""
        # pylint: disable=protected-access
        b_path = os.path.join(self.temp_dir, "empty_backend.db")
        backend = SqliteBackend(db_file=b_path)
        try:
            with self.assertRaises(KeyError):
                _ = backend["missing"]
            with self.assertRaises(KeyError):
                del backend["missing"]
            backend._insert("direct_k", "direct_v")
            self.assertTrue(backend._has_db_rows)
            self.assertEqual(backend["direct_k"], "direct_v")
        finally:
            backend.close()

        # SqliteSetBackend with db_file=None and unlink_old_db=True
        s_none = SqliteSetBackend(db_file=None)
        none_path = s_none.db_file_path()
        s_none.close()
        if os.path.exists(none_path):
            os.unlink(none_path)

        s_path = os.path.join(self.temp_dir, "set_edge.db")
        self._create_file("set_edge.db", b"")
        sb = SqliteSetBackend(db_file=s_path, unlink_old_db=True)
        try:
            with self.assertRaises(KeyError):
                sb._get_key_id("missing_empty")
            sb.remove_batch([])
            sb.update_batch([])
            sb.add("buffered_1")
            sb.remove_batch(["buffered_1"])
            self.assertNotIn("buffered_1", sb)

            sb.add("buffered_2")
            sb.remove("buffered_2")
            self.assertNotIn("buffered_2", sb)

            sb.add("pending_up")
            sb.update_batch(["pending_up", "committed_1"])
            self.assertIn("committed_1", sb)
            self.assertNotIn("never_added", sb)

            sb._batch_size = 1
            sb.remove("committed_1")
            self.assertNotIn("committed_1", sb)

            sb.update_batch(["err_key"])
            mock_conn = MagicMock()
            mock_conn.executemany.side_effect = sqlite3.OperationalError("boom")
            with patch.object(SqliteSetBackend, "conn", new=mock_conn):
                with self.assertRaises(sqlite3.OperationalError):
                    sb.remove_batch(["err_key"])
                mock_conn.rollback.assert_called_once()
        finally:
            sb.close()

        # PersistentSet with a custom backend lacking remove_batch
        custom_backend = {"a", "b"}
        ps = PersistentSet(backend=custom_backend)
        ps.discard_batch(["a"])
        self.assertNotIn("a", custom_backend)

    def test_manifest_and_path_rules_edge_cases(self):
        """Cover _stage_sqlite_file on 0-byte file, _discard_from_read_sources, and path_rules."""
        # pylint: disable=protected-access
        zero_src = self._create_file("zero.db", b"")
        zero_dst = os.path.join(self.temp_dir, "sub_stage", "zero_copy.db")
        _stage_sqlite_file(zero_src, zero_dst)
        self.assertTrue(os.path.exists(zero_dst))

        m = Manifest(None, temp_directory=self.temp_dir)
        try:
            m._discard_from_read_sources([])
            m._add_to_read_sources([])
            m.read_sources = {"/p/1", "/p/2"}
            m._discard_from_read_sources(["/p/1"])
            self.assertEqual(m.read_sources, {"/p/2"})
        finally:
            m.close()

        self.assertEqual(strip_read_path_prefix("/a/b.txt", None), ("a/b.txt", False))
        self.assertEqual(strip_read_path_prefix("/a/b.txt", ["/"]), ("a/b.txt", True))
        self.assertEqual(strip_read_path_prefix("/a", ["/a"]), ("", True))
        self.assertEqual(
            strip_read_path_prefix("/unrelated/b.txt", ["/data"]),
            ("unrelated/b.txt", False),
        )

    def test_threads_and_core_edge_cases(self):
        """Cover edge cases in threads.py and core.py (--no-walk with --ignore-old-collisions)."""
        # pylint: disable=protected-access
        f_a = self._create_file("dupes/a.txt", b"dupe_content")
        self._create_file("dupes/b.txt", b"dupe_content")
        m_in = os.path.join(self.temp_dir, "dupes_in.db")
        r_out = os.path.join(self.temp_dir, "dupes_report.csv")

        run_dupe_copy(
            read_from_path=[os.path.join(self.temp_dir, "dupes")],
            manifest_out_path=m_in,
        )
        run_dupe_copy(
            manifests_in_paths=[m_in],
            csv_report_path=r_out,
            no_walk=True,
            ignore_old_collisions=True,
        )
        self.assertTrue(os.path.exists(r_out))

        # Relative path in _is_file_processing_required when abspath is in already_processed
        rel_path = os.path.relpath(f_a, os.getcwd())
        self.assertFalse(
            _is_file_processing_required(
                rel_path, {os.path.abspath(f_a)}, None, None, None
            )
        )

        # _mark_path_seen with None and without lock
        self.assertTrue(_mark_path_seen(f_a, None, None))
        seen: set = set()
        self.assertTrue(_mark_path_seen(f_a, seen, None))
        self.assertFalse(_mark_path_seen(f_a, seen, None))

        # Direct _copy_file samefile and cleanup OSError
        pq: "queue.PriorityQueue" = queue.PriorityQueue()
        self.assertFalse(_copy_file(f_a, f_a, False, pq))

        dest_fail = os.path.join(self.temp_dir, "fail_clean.txt")

        def fail_copy_and_leave(_s, d):
            with open(d, "wb") as f:
                f.write(b"x")
            raise OSError("copy failed")

        with (
            patch("dedupe_copy.threads.shutil.copyfile", side_effect=fail_copy_and_leave),
            patch("dedupe_copy.threads.os.remove", side_effect=OSError("unlink failed")),
        ):
            self.assertFalse(_copy_file(f_a, dest_fail, False, pq))

        # CopyThread._resolve_destination_path and _handle_delete_on_copy edge cases
        ct = CopyThread(
            queue.Queue(),
            threading.Event(),
            copy_config=CopyConfig(
                target_path=self.temp_dir,
                read_paths=[self.temp_dir],
                rename_on_collision=True,
            ),
            progress_queue=pq,
        )
        existing_dest = self._create_file("existing_dest.txt", b"diff")
        with patch("dedupe_copy.threads.os.path.samefile", side_effect=OSError("err")):
            with patch(
                "dedupe_copy.threads.os.path.getsize", side_effect=OSError("err")
            ):
                resolved = ct._resolve_destination_path(f_a, existing_dest)
                self.assertTrue(resolved and resolved.endswith("existing_dest_1.txt"))

        ct._handle_delete_on_copy(f_a, f_a)
        self.assertTrue(os.path.exists(f_a))

    def test_walk_and_distribute_work_edge_cases(self):
        """Cover seen-file skip in distribute_work, CopyThread fallback, and WalkThread."""
        # pylint: disable=protected-access
        f_seen = self._create_file("seen_dir/file.txt", b"data")
        work_q: "queue.Queue[str]" = queue.Queue()
        walk_q: "queue.Queue[str]" = queue.Queue()
        seen = {os.path.normcase(os.path.abspath(f_seen))}
        cfg = DistributeWorkConfig(
            already_processed=set(),
            walk_config=WalkConfig(),
            progress_queue=None,
            work_queue=work_q,
            walk_queue=walk_q,
            seen_paths=seen,
            seen_lock=threading.Lock(),
        )
        distribute_work(os.path.dirname(f_seen), cfg)
        self.assertTrue(work_q.empty())

        # CopyThread._process_copy_task when extension_matcher is None
        copy_cfg = CopyConfig(
            target_path=self.temp_dir,
            read_paths=[self.temp_dir],
            extensions=["*.jpg"],
        )
        copy_cfg.extension_matcher = None  # type: ignore[assignment]
        ct = CopyThread(queue.Queue(), threading.Event(), copy_config=copy_cfg)
        ct._process_copy_task(f_seen, "2026_01", 4)

        # WalkThread receiving a file path instead of a directory path
        pq: "queue.PriorityQueue" = queue.PriorityQueue()
        stop_ev = threading.Event()
        stop_ev.set()
        walk_q.put(f_seen)
        wt = WalkThread(
            walk_q,
            stop_ev,
            walk_config=WalkConfig(),
            work_queue=work_q,
            already_processed=set(),
            progress_queue=pq,
        )
        wt.run()
        self.assertFalse(pq.empty())

    def test_cli_rename_on_collision_argument_parsing(self):
        """CLI parser accepts --rename-on-collision and passes it to run_dupe_copy."""
        with (
            patch(
                "sys.argv",
                [
                    "dedupecopy",
                    "-p",
                    self.temp_dir,
                    "-c",
                    os.path.join(self.temp_dir, "out"),
                    "--rename-on-collision",
                ],
            ),
            patch("dedupe_copy.bin.dedupecopy_cli.setup_logging"),
            patch("dedupe_copy.bin.dedupecopy_cli.run_dupe_copy") as mock_run,
        ):
            run_cli()
            mock_run.assert_called_once()
            _, kwargs = mock_run.call_args
            self.assertTrue(kwargs.get("rename_on_collision"))

    def test_input_manifest_never_modified_in_place(self):
        """Loading an input manifest (-i) must never mutate the original files on disk."""
        dir1 = os.path.join(self.temp_dir, "dir1")
        dir2 = os.path.join(self.temp_dir, "dir2")
        self._create_file("dir1/one.txt", b"file one")
        self._create_file("dir2/two.txt", b"file two")

        input_manifest = os.path.join(self.temp_dir, "input.db")
        output_manifest = os.path.join(self.temp_dir, "output.db")

        # Step 1: Create initial manifest scanning dir1
        run_dupe_copy(read_from_path=[dir1], manifest_out_path=input_manifest)

        # Verify initial manifest has 1 file
        m_before = Manifest(
            input_manifest, temp_directory=os.path.join(self.temp_dir, "check1")
        )
        try:
            self.assertEqual(len(m_before.read_sources), 1)
            self.assertEqual(len(m_before.md5_data), 1)
        finally:
            m_before.close()

        # Step 2: Run with -i input_manifest and scan dir2 into output_manifest
        run_dupe_copy(
            read_from_path=[dir2],
            manifests_in_paths=[input_manifest],
            manifest_out_path=output_manifest,
        )

        # Verify input_manifest on disk STILL has only the 1 original file from dir1!
        m_after = Manifest(
            input_manifest, temp_directory=os.path.join(self.temp_dir, "check2")
        )
        try:
            self.assertEqual(len(m_after.read_sources), 1)
            self.assertEqual(len(m_after.md5_data), 1)
        finally:
            m_after.close()

        # And output_manifest has both files (2 files total)
        m_out = Manifest(
            output_manifest, temp_directory=os.path.join(self.temp_dir, "check3")
        )
        try:
            self.assertEqual(len(m_out.read_sources), 2)
            self.assertEqual(len(m_out.md5_data), 2)
        finally:
            m_out.close()

        # Also verify passing identical input/output manifest path as a string raises ValueError
        with self.assertRaises(ValueError):
            run_dupe_copy(
                read_from_path=[dir2],
                manifests_in_paths=input_manifest,
                manifest_out_path=input_manifest,
            )

    def test_extension_matcher_normalizes_extensions_and_no_walk_empty_files(self):
        """ExtensionMatcher normalizes raw extensions and --no-walk respects dedupe_empty=False."""
        matcher = ExtensionMatcher(["jpg", ".PNG", "*.gif"])
        self.assertTrue(matcher.match("photo.JPG"))
        self.assertTrue(matcher.match("icon.png"))
        self.assertTrue(matcher.match("anim.gif"))
        self.assertFalse(matcher.match("doc.pdf"))

        # Test 0-byte files with --no-walk and dedupe_empty=False
        empty1 = self._create_file("empty_dir/e1.txt", b"")
        empty2 = self._create_file("empty_dir/e2.txt", b"")
        manifest_1 = os.path.join(self.temp_dir, "empty1.db")
        manifest_2 = os.path.join(self.temp_dir, "empty2.db")

        run_dupe_copy(
            read_from_path=[os.path.join(self.temp_dir, "empty_dir")],
            manifest_out_path=manifest_1,
            dedupe_empty=False,
        )

        # Now run --no-walk with --delete and dedupe_empty=False: empty files must NOT be deleted
        run_dupe_copy(
            manifests_in_paths=[manifest_1],
            manifest_out_path=manifest_2,
            no_walk=True,
            delete_duplicates=True,
            dedupe_empty=False,
        )
        self.assertTrue(os.path.exists(empty1))
        self.assertTrue(os.path.exists(empty2))

        with patch("dedupe_copy.utils.time.sleep") as mock_sleep:
            _throttle_puts(0)
            _throttle_puts(MAX_TARGET_QUEUE_SIZE - 1)
            mock_sleep.assert_not_called()
            _throttle_puts(MAX_TARGET_QUEUE_SIZE)
            mock_sleep.assert_called_once()
