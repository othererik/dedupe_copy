"""Performance regression test suite and benchmark runner for DedupeCopy.

Marked with `@pytest.mark.perf` so normal CI runs (`pytest`) skip it by default.
Run before release with:

    pytest -m perf

Can also be executed directly as a CLI benchmark tool to record a baseline JSON
file or compare current performance against a saved baseline:

    python -m dedupe_copy.test.test_performance_regression --save-baseline perf_baseline.json
    python -m dedupe_copy.test.test_performance_regression --compare-baseline perf_baseline.json
"""

import argparse
import json
import os
import queue
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from typing import Any, Callable
from unittest import mock

import pytest

from dedupe_copy.config import WalkConfig
from dedupe_copy.core import run_dupe_copy
from dedupe_copy.disk_cache_dict import (
    DefaultCacheDict,
    PersistentSet,
    SqliteBackend,
    SqliteSetBackend,
)
from dedupe_copy.manifest import Manifest
from dedupe_copy.threads import (
    DistributeWorkConfig,
    ResultProcessor,
    distribute_work,
)

pytestmark = pytest.mark.perf

# pylint: disable=protected-access


class TestPerformanceInvariants(unittest.TestCase):
    """Deterministic algorithmic and I/O call-count regression tests."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="perf_reg_")

    def tearDown(self) -> None:
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

    def test_sqlite_backends_have_no_triggers_or_redundant_hash_index(self) -> None:
        """SqliteBackend and SqliteSetBackend must not create per-row triggers or hash_index,
        and must drop legacy ones when opening an older database file.
        """
        dict_db = os.path.join(self.temp_dir, "legacy_dict.db")
        # Simulate a legacy <=1.2.5 database with triggers and hash_index
        conn = sqlite3.connect(dict_db)
        conn.execute(
            "CREATE TABLE sql_dict_table (key BLOB PRIMARY KEY, hash INTEGER, value BLOB);"
        )
        conn.execute(
            "CREATE INDEX sql_dict_table_hash_index ON sql_dict_table(hash);"
        )
        conn.execute(
            "CREATE TABLE _meta_info (tablename TEXT PRIMARY KEY, count INTEGER);"
        )
        conn.execute(
            "CREATE TRIGGER sql_dict_table_ins_count AFTER INSERT ON sql_dict_table "
            "BEGIN UPDATE _meta_info SET count = count + 1 WHERE tablename = 'sql_dict_table'; END;"
        )
        conn.execute(
            "CREATE TRIGGER sql_dict_table_del_count AFTER DELETE ON sql_dict_table "
            "BEGIN UPDATE _meta_info SET count = count - 1 WHERE tablename = 'sql_dict_table'; END;"
        )
        conn.commit()
        conn.close()

        backend = SqliteBackend(db_file=dict_db)
        try:
            master_rows = backend.conn.execute(
                "SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'index');"
            ).fetchall()
            names = {name for _, name in master_rows}
            self.assertNotIn("sql_dict_table_ins_count", names)
            self.assertNotIn("sql_dict_table_del_count", names)
            self.assertNotIn("sql_dict_table_hash_index", names)
        finally:
            backend.close()

        set_db = os.path.join(self.temp_dir, "set.db")
        set_backend = SqliteSetBackend(db_file=set_db)
        try:
            master_rows = set_backend.conn.execute(
                "SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'index');"
            ).fetchall()
            names = {name for _, name in master_rows}
            self.assertNotIn("sql_set_table_ins_count", names)
            self.assertNotIn("sql_set_table_del_count", names)
            self.assertNotIn("sql_set_table_hash_index", names)
        finally:
            set_backend.close()

    def test_persistent_set_update_batches_commits(self) -> None:
        """PersistentSet.update() must buffer in _write_batch instead of committing every call."""
        db_path = os.path.join(self.temp_dir, "pset_batch.db")
        pset = PersistentSet(max_size=1000, db_file=db_path)
        try:
            # First 1,000 items fit in _cache; next batch forces DB usage
            pset.update([f"/path/init_{i}" for i in range(1000)])
            pset.save()
            self.assertTrue(pset._db._has_db_rows)

            # Simulate ResultProcessor calling update() 20 times with 500 paths each.
            # With _batch_size = 10,000, _commit_batch should only run once!
            with mock.patch.object(
                pset._db, "_commit_batch", wraps=pset._db._commit_batch
            ) as spy_commit:
                for batch_idx in range(20):
                    batch = [f"/path/b_{batch_idx}_{i}" for i in range(500)]
                    pset.update(batch)
                self.assertLessEqual(
                    spy_commit.call_count,
                    2,
                    f"Expected <= 2 batch commits for 20 update() calls, "
                    f"got {spy_commit.call_count}",
                )
            self.assertEqual(len(pset), 11000)
        finally:
            pset.close()

    def test_result_processor_single_file_skips_abspath_and_collisions_are_linear(
        self,
    ) -> None:
        """_merge_files_for_hash skips abspath on single new files and is O(K) on collisions."""
        manifest_path = os.path.join(self.temp_dir, "rp_manifest.db")
        manifest = Manifest(
            None, save_path=manifest_path, temp_directory=self.temp_dir
        )
        collisions: dict[str, Any] = {}
        try:
            processor = ResultProcessor(
                threading.Event(),
                queue.Queue(),
                collisions,
                manifest,
                dedupe_empty=True,
            )

            # 1. Single-file new hashes must not call os.path.abspath at all
            with mock.patch(
                "dedupe_copy.threads.os.path.abspath", wraps=os.path.abspath
            ) as spy_abspath:
                for i in range(500):
                    processor._process_single_result(
                        f"unique_md5_{i}", 128, 1000.0, f"/tmp/u_{i}.txt"
                    )
                processor._commit_batch()
                self.assertEqual(
                    spy_abspath.call_count,
                    0,
                    f"Expected 0 abspath calls for 500 unique single-file hashes, "
                    f"got {spy_abspath.call_count}",
                )

            # 2. 500 colliding files across 50 batches for the same hash must make O(K) (~500)
            # abspath calls, not O(K^2) (~125,000) calls.
            with mock.patch(
                "dedupe_copy.threads.os.path.abspath", wraps=os.path.abspath
            ) as spy_abspath:
                for batch_idx in range(50):
                    for j in range(10):
                        idx = batch_idx * 10 + j
                        processor._process_single_result(
                            "colliding_md5", 0, 1000.0, f"/tmp/empty_{idx}.txt"
                        )
                    processor._commit_batch()
                self.assertLessEqual(
                    spy_abspath.call_count,
                    550,
                    f"Quadratic abspath regression detected: "
                    f"{spy_abspath.call_count} calls for 500 files",
                )
            self.assertEqual(len(manifest.md5_data["colliding_md5"]), 500)
        finally:
            manifest.close()

    def test_distribute_work_normalizes_directory_once(self) -> None:
        """distribute_work calls os.path.abspath once per directory instead of per file."""
        scan_dir = os.path.join(self.temp_dir, "scan_target")
        os.makedirs(scan_dir, exist_ok=True)
        for i in range(200):
            with open(os.path.join(scan_dir, f"f_{i}.txt"), "wb") as fh:
                fh.write(b"x")

        work_q: "queue.Queue[str]" = queue.Queue()
        walk_q: "queue.Queue[str]" = queue.Queue()
        seen_paths: set[str] = set()
        config = DistributeWorkConfig(
            already_processed=set(),
            walk_config=WalkConfig(),
            progress_queue=None,
            work_queue=work_q,
            walk_queue=walk_q,
            seen_paths=seen_paths,
            seen_lock=threading.Lock(),
        )

        with mock.patch(
            "dedupe_copy.threads.os.path.abspath", wraps=os.path.abspath
        ) as spy_abspath:
            distribute_work(scan_dir, config)
            self.assertEqual(
                spy_abspath.call_count,
                1,
                f"Expected 1 abspath call for directory of 200 files, "
                f"got {spy_abspath.call_count}",
            )
        self.assertEqual(work_q.qsize(), 200)
        self.assertEqual(len(seen_paths), 200)

    def test_run_dupe_copy_skips_redundant_populate_read_sources_on_final_save(
        self,
    ) -> None:
        """Final manifest save in run_dupe_copy must not call _populate_read_sources()."""
        src_dir = os.path.join(self.temp_dir, "src")
        os.makedirs(src_dir, exist_ok=True)
        for i in range(20):
            with open(os.path.join(src_dir, f"file_{i}.txt"), "wb") as fh:
                fh.write(f"content_{i}".encode("utf-8"))

        manifest_out = os.path.join(self.temp_dir, "out_manifest.db")
        with mock.patch.object(
            Manifest,
            "_populate_read_sources",
            wraps=Manifest._populate_read_sources,
        ) as spy_populate:
            rc = run_dupe_copy(
                read_from_path=[src_dir],
                manifest_out_path=manifest_out,
                walk_threads=2,
                read_threads=2,
            )
            self.assertEqual(rc, 0)
            self.assertEqual(
                spy_populate.call_count,
                0,
                "Manifest._populate_read_sources should not be called during normal walk + save",
            )

        loaded = Manifest(manifest_out, temp_directory=self.temp_dir)
        try:
            self.assertEqual(len(loaded.md5_data), 20)
            self.assertEqual(len(loaded.read_sources), 20)
        finally:
            loaded.close()

    def test_sqlite_clear_uses_fast_truncate(self) -> None:
        """Clearing 20,000 rows from SqliteBackend should complete in < 0.25s."""
        dict_db = os.path.join(self.temp_dir, "fast_clear.dict")
        backend = SqliteBackend(db_file=dict_db)
        try:
            backend.update_batch({f"k_{i}": [f"/p/{i}", i, 1.0] for i in range(20000)})
            self.assertEqual(len(backend), 20000)
            t0 = time.perf_counter()
            backend.clear()
            elapsed = time.perf_counter() - t0
            self.assertEqual(len(backend), 0)
            self.assertLess(elapsed, 0.25, f"SqliteBackend.clear() took {elapsed:.3f}s")
        finally:
            backend.close()


# ---------------------------------------------------------------------------
# Quantitative Benchmark Suite (used by both pytest -m perf and CLI comparison)
# ---------------------------------------------------------------------------


def _bench_sqlite_dict_upsert_and_clear(temp_dir: str) -> float:
    db_path = os.path.join(temp_dir, "bench_dict.db")
    cd = DefaultCacheDict(list, max_size=5000, db_file=db_path)
    t0 = time.perf_counter()
    try:
        for i in range(15000):
            cd[f"hash_{i}"] = [(f"/data/dir/file_{i}.bin", i * 10, 1700000000.0)]
        cd.save()
        _ = len(cd)
        cd.clear()
    finally:
        cd.close()
    return time.perf_counter() - t0


def _bench_persistent_set_batched_updates(temp_dir: str) -> float:
    db_path = os.path.join(temp_dir, "bench_pset.db")
    pset = PersistentSet(max_size=5000, db_file=db_path)
    t0 = time.perf_counter()
    try:
        for batch_idx in range(30):
            pset.update([f"/data/src/batch_{batch_idx}/file_{i}.jpg" for i in range(1000)])
        pset.save()
        _ = len(pset)
    finally:
        pset.close()
    return time.perf_counter() - t0


def _bench_result_processor_mixed_workload(temp_dir: str) -> float:
    manifest_path = os.path.join(temp_dir, "bench_rp_manifest.db")
    manifest = Manifest(None, save_path=manifest_path, temp_directory=temp_dir)
    collisions = DefaultCacheDict(
        list, db_file=os.path.join(temp_dir, "bench_collisions.db"), max_size=50000
    )
    t0 = time.perf_counter()
    try:
        processor = ResultProcessor(
            threading.Event(),
            queue.Queue(),
            collisions,
            manifest,
            dedupe_empty=True,
        )
        # 10,000 unique files + 2,000 colliding files across 12 batches
        for i in range(10000):
            processor._process_single_result(
                f"uniq_{i}", 1024, 1700000000.0, f"/mnt/archive/u_{i}.dat"
            )
            if i % 5 == 0:
                processor._process_single_result(
                    "empty_md5", 0, 1700000000.0, f"/mnt/archive/empty_{i}.dat"
                )
        processor._commit_batch()
        manifest.save(rebuild_sources=False)
    finally:
        collisions.close()
        manifest.close()
    return time.perf_counter() - t0


def _bench_distribute_work_directory_scan(temp_dir: str) -> float:
    scan_dir = os.path.join(temp_dir, "bench_scan")
    os.makedirs(scan_dir, exist_ok=True)
    for i in range(1000):
        with open(os.path.join(scan_dir, f"item_{i}.txt"), "wb") as fh:
            fh.write(b"x")

    walk_config = WalkConfig(extensions=[".txt"], ignore=["*.tmp"])
    t0 = time.perf_counter()
    for _ in range(10):
        work_q: "queue.Queue[str]" = queue.Queue()
        walk_q: "queue.Queue[str]" = queue.Queue()
        config = DistributeWorkConfig(
            already_processed=set(),
            walk_config=walk_config,
            progress_queue=None,
            work_queue=work_q,
            walk_queue=walk_q,
            seen_paths=set(),
            seen_lock=threading.Lock(),
        )
        distribute_work(scan_dir, config)
    return time.perf_counter() - t0


BENCHMARKS: dict[str, Callable[[str], float]] = {
    "sqlite_dict_upsert_and_clear_15k": _bench_sqlite_dict_upsert_and_clear,
    "persistent_set_batched_updates_30k": _bench_persistent_set_batched_updates,
    "result_processor_mixed_12k": _bench_result_processor_mixed_workload,
    "distribute_work_scan_10x1k": _bench_distribute_work_directory_scan,
}


def run_all_benchmarks(repeats: int = 3) -> dict[str, float]:
    """Runs each benchmark `repeats` times and returns the minimum elapsed time (seconds)."""
    results: dict[str, float] = {}
    for name, func in BENCHMARKS.items():
        timings: list[float] = []
        for _ in range(repeats):
            with tempfile.TemporaryDirectory(prefix=f"bench_{name}_") as tmp_dir:
                timings.append(func(tmp_dir))
        results[name] = min(timings)
    return results


class TestBenchmarkGuardrails(unittest.TestCase):
    """Runs the benchmark suite and enforces generous upper-bound time budgets."""

    def test_benchmarks_within_time_budgets(self) -> None:
        """Ensure none of the core benchmarks exceed their regression ceiling."""
        results = run_all_benchmarks(repeats=1)
        budgets = {
            "sqlite_dict_upsert_and_clear_15k": 3.0,
            "persistent_set_batched_updates_30k": 3.0,
            "result_processor_mixed_12k": 3.0,
            "distribute_work_scan_10x1k": 3.0,
        }
        for name, elapsed in results.items():
            self.assertLess(
                elapsed,
                budgets[name],
                f"Benchmark {name} took {elapsed:.3f}s, exceeding budget {budgets[name]:.3f}s",
            )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for saving or comparing benchmark baselines."""
    parser = argparse.ArgumentParser(
        description="Run DedupeCopy performance regression benchmarks."
    )
    parser.add_argument(
        "--save-baseline",
        metavar="FILE",
        help="Save benchmark results (JSON) to the specified path.",
    )
    parser.add_argument(
        "--compare-baseline",
        metavar="FILE",
        help="Compare current benchmark results against a saved baseline JSON file.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=25.0,
        help="Allowed percentage slowdown before failing comparison (default: 25.0).",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of repetitions per benchmark (default: 3, taking minimum).",
    )
    args = parser.parse_args(argv)

    current = run_all_benchmarks(repeats=args.repeats)
    print(f"{'Benchmark':<38} {'Current (s)':>12}")
    print("-" * 52)
    for name, seconds in current.items():
        print(f"{name:<38} {seconds:>12.4f}")

    if args.save_baseline:
        with open(args.save_baseline, "w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2, sort_keys=True)
        print(f"\nSaved baseline to {args.save_baseline}")

    if args.compare_baseline:
        with open(args.compare_baseline, "r", encoding="utf-8") as fh:
            baseline: dict[str, float] = json.load(fh)
        print(
            f"\nComparing against {args.compare_baseline} (threshold: +{args.threshold:.1f}%):"
        )
        print(
            f"{'Benchmark':<38} {'Baseline':>10} {'Current':>10} {'Delta':>10} {'Status':>8}"
        )
        print("-" * 80)
        failed = False
        for name, curr_val in current.items():
            base_val = baseline.get(name)
            if not base_val or base_val <= 0:
                continue
            pct = ((curr_val - base_val) / base_val) * 100.0
            regressed = pct > args.threshold
            if regressed:
                failed = True
            status = "FAIL" if regressed else "OK"
            print(
                f"{name:<38} {base_val:>9.4f}s {curr_val:>9.4f}s {pct:>+9.1f}% {status:>8}"
            )
        if failed:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
