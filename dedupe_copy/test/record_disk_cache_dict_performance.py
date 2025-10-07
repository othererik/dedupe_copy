"""These are not really 'tests', but just give a relative measure of
the time to perform various actions between a normal python defaultdict and
the disk cache version.

This is all low grade not to be fully believed performance estimation!
"""
# pylint: disable=too-many-arguments
import collections
import contextlib
import os
import random
import time
from dataclasses import dataclass
from typing import Dict, Any, IO, List, Tuple, Callable, Iterator, Optional

from dedupe_copy.test import utils
from dedupe_copy import disk_cache_dict

# data set sizes, should be one larger and one smaller than cache sizes
SMALL_SET = 1000
LARGE_SET = 10000

# cache sizes - this is the max cache size for the dcd
SMALL_CACHE = 10
LARGE_CACHE = 100000

disk_cache_dict.DEBUG = False


@dataclass
class PerfTestConfig:
    """Configuration for a performance test run."""

    item_count: int
    lru: bool
    max_size: int
    backend: Any
    in_cache: bool
    logfd: IO


@contextlib.contextmanager
def temp_db():
    """Context manager for creating a temporary database file."""
    temp_dir = utils.make_temp_dir("dcd_temp")
    db_file = os.path.join(temp_dir, "perf_db.dict")
    try:
        yield db_file
    finally:
        utils.remove_dir(temp_dir)


def time_once(func):
    """Adds an additional return value of the run time"""

    def time_func(*args, **kwargs):
        # print('\tstart: {0}'.format(func.__name__))
        start = time.time()
        func(*args, **kwargs)
        total = time.time() - start
        # print('\tend: {0} {1}s'.format(func.__name__, total)
        return total

    return time_func


@time_once
def populate(container, items):
    """Populate container with items."""
    for key, value in items:
        container[key] = value


@time_once
def random_access(container, keys):
    """Perform random access operations on container."""
    for _ in range(len(keys)):
        _ = container[random.choice(keys)]


@time_once
def sequential_access(container, keys):
    """Perform sequential access operations on container."""
    for test_key in keys:
        _ = container[test_key]


@time_once
def random_update(container, keys):
    """Perform random update operations on container."""
    for i in range(len(keys)):
        # doing an int to incur some call cost
        container[random.choice(keys)] = int(i)


@time_once
def sequential_update(container, keys):
    """Perform sequential update operations on container."""
    for test_key in keys:
        container[test_key] = int(test_key)


def _delete(contaner, key):
    try:
        del contaner[key]
    except KeyError:
        pass


def _update(container, key):
    container[key] = random.getrandbits(16)


def _add(container, _):
    nkey = "".join([random.choice("abcdefghijklmnopqrstuvwzyz") for _ in range(10)])
    container[nkey] = nkey
    return nkey


def _get(container, key):
    """Get an item from container."""
    _ = container[key]


@time_once
def random_actions(container, keys):
    """Perform random actions on container."""
    actions = [_delete, _update, _add, _get]
    for _ in range(5000):
        action = random.choice(actions)
        key = random.choice(keys)
        nkey = action(container, key)
        if nkey and nkey not in keys:
            keys.append(nkey)


@time_once
def iterate(container, keys):
    """Iterate over container."""
    citer = iter(container)
    for _ in keys:
        next(citer)


def log_perf(
    name: str,
    py_time: float,
    dcd_time: float,
    log_fd: IO,
    log_params: Dict[str, Any],
):
    """Log performance test results to console and optionally to file."""
    percent = ((dcd_time - py_time) / py_time) * 100 if py_time else 0
    print(
        f"{name:<30}\tdifference: {percent:<5.2f}%\tpy: {py_time:.4f}s\t"
        f"dcd: {dcd_time:.4f}s"
    )
    log_spec = (
        "{name}, {percent}, {py}, {dcd}, {lru}, "
        "{in_cache}, {backend}, {item_count}, {max_size}\n"
    )
    log_params.update(name=name, percent=percent, py=py_time, dcd=dcd_time)
    log_fd.write(log_spec.format(**log_params))


def run_and_log_test(
    name: str,
    test_func: Callable,
    pydict: Dict,
    dcd: disk_cache_dict.DefaultCacheDict,
    keys: List[str],
    config: "PerfTestConfig",
    log_params: Dict[str, Any],
    items: Optional[List[Tuple[str, str]]] = None,
) -> Tuple[float, float]:
    """Runs a performance test and logs the results."""
    args_py = (pydict, items) if "populate" in name.lower() else (pydict, keys)
    args_dcd = (dcd, items) if "populate" in name.lower() else (dcd, keys)

    py_time = test_func(*args_py)
    dcd_time = test_func(*args_dcd)
    log_perf(name, py_time, dcd_time, config.logfd, log_params)
    return py_time, dcd_time


def _run_perf_test(config: PerfTestConfig):
    """Runs a single performance test configuration."""
    keys = [str(i) for i in range(config.item_count)]
    items = [(str(i), str(i)) for i in keys]
    print(
        f"Running lru: {config.lru} max_size: {config.max_size} "
        f"backend: {config.backend or 'default'} item_count: {config.item_count} "
        f"in_cache_only: {config.in_cache}"
    )
    with temp_db() as db_file:
        pydict = collections.defaultdict(list)
        dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=config.max_size,
            db_file=db_file,
            lru=config.lru,
            backend=config.backend,
        )
        log_params = {
            "item_count": config.item_count,
            "lru": config.lru,
            "max_size": config.max_size,
            "backend": config.backend,
            "in_cache": config.in_cache,
        }

        run_and_log_test(
            "Populate",
            populate,
            pydict,
            dcd,
            keys,
            config,
            log_params,
            items=items,
        )

        py_sum = dcd_sum = 0

        if config.in_cache:
            # pylint: disable=protected-access
            test_keys = list(dcd._cache.keys())
        else:
            test_keys = keys

        tests_to_run = {
            "Rand Access": random_access,
            "Rand Update": random_update,
            "Sequential Access": sequential_access,
            "Sequential Update": sequential_update,
            "Iterate": iterate,
            "Random Actions": random_actions,
        }

        for name, func in tests_to_run.items():
            py_time, dcd_time = run_and_log_test(
                name, func, pydict, dcd, test_keys, config, log_params
            )
            py_sum += py_time
            dcd_sum += dcd_time

        if py_sum:
            print(f"Run Average: {(dcd_sum / py_sum) * 100}%\n\n")


def _generate_test_configs(logfd: IO) -> Iterator[PerfTestConfig]:
    """Generate test configurations."""
    item_counts = [SMALL_SET, LARGE_SET]
    max_sizes = [SMALL_CACHE, LARGE_CACHE]
    backends = [None]
    lrus = [True, False]
    in_caches = [True, False]

    for item_count in item_counts:
        for max_size in max_sizes:
            for backend in backends:
                for lru in lrus:
                    for in_cache in in_caches:
                        if not item_count or not max_size:
                            continue
                        if in_cache and max_size < LARGE_CACHE:
                            continue
                        yield PerfTestConfig(
                            item_count, lru, max_size, backend, in_cache, logfd
                        )


def gen_tests() -> Iterator[PerfTestConfig]:
    """Generate test configurations and write results to CSV."""
    try:
        with open("perflog.csv", "a", encoding="utf-8") as fd:
            fd.write(
                "name, percent, py, dcd, lru, in_cache, backend, item_count, max_size\n"
            )
            yield from _generate_test_configs(fd)
    except IOError:
        pass


def main():
    """Run performance tests on disk cache dict."""
    for config in gen_tests():
        _run_perf_test(config)


if __name__ == "__main__":
    main()