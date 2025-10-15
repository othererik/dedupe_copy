"""These are not really 'tests', but just give a relative measure of
the time to perform various actions between a normal python defaultdict and
the disk cache version. This is all low grade not to be fully believed
performance estimation!
"""

import collections
import contextlib
import os
import random
import statistics
import time

from dedupe_copy.test import utils
from dedupe_copy import disk_cache_dict


def generate_string_data(item_count):
    """Generate string key/value pairs."""
    keys = [str(i) for i in range(item_count)]
    items = [(k, f"value_{k}") for k in keys]
    return keys, items


def generate_int_data(item_count):
    """Generate integer key/value pairs."""
    keys = list(range(item_count))
    items = [(k, k * 10) for k in keys]
    return keys, items


def generate_complex_data(item_count):
    """Generate string keys and complex dict values."""
    keys = [str(i) for i in range(item_count)]
    items = [
        (k, {"id": k, "value": random.random(), "payload": "x" * 20}) for k in keys
    ]
    return keys, items


DATA_TYPES = {
    "string": generate_string_data,
    "int": generate_int_data,
    "complex": generate_complex_data,
}

# number of times to run each test configuration
NUM_RUNS = 3

# data set sizes, should be one larger and one smaller than cache sizes
SMALL_SET = 1000
LARGE_SET = 10000

# cache sizes - this is the max cache size for the dcd
SMALL_CACHE = 10
LARGE_CACHE = 100000

disk_cache_dict.DEBUG = False


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
        # print '\tstart: {0}'.format(func.__name__)
        start = time.time()
        func(*args, **kwargs)
        total = time.time() - start
        # print '\tend: {0} {1}s'.format(func.__name__, total)
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


def log(  # pylint: disable=too-many-arguments,too-many-locals
    name,
    py_mean,
    py_std,
    dcd_mean,
    dcd_std,
    *,
    log_fd=None,
    lru=None,
    in_cache=None,
    backend=None,
    item_count=None,
    max_size=None,
    data_type=None,
):
    """Log performance test results to console and optionally to file."""
    percent = ((dcd_mean - py_mean) / py_mean) * 100 if py_mean > 0 else 0
    print(
        f"{name:<20}\t"
        f"d: {percent:<5.2f}%\t"
        f"py: {py_mean:.4f}s (±{py_std:.4f})\t"
        f"dcd: {dcd_mean:.4f}s (±{dcd_std:.4f})"
    )
    log_spec = (
        "{name},{percent},{py_mean},{py_std},{dcd_mean},{dcd_std},"
        "{lru},{in_cache},{backend},{item_count},{max_size},{data_type}\n"
    )
    if log_fd:
        log_fd.write(
            log_spec.format(
                name=name,
                percent=percent,
                py_mean=py_mean,
                py_std=py_std,
                dcd_mean=dcd_mean,
                dcd_std=dcd_std,
                lru=lru,
                max_size=max_size,
                in_cache=in_cache,
                backend=backend,
                item_count=item_count,
                data_type=data_type,
            )
        )


# pylint: disable=too-many-nested-blocks
def gen_tests():
    """Generate test configurations and write results to CSV."""
    with open("perflog.csv", "w", encoding="utf-8") as fd:
        fd.write(
            "name,percent,py_mean,py_std,dcd_mean,dcd_std,lru,in_cache,"
            "backend,item_count,max_size,data_type\n"
        )
        for data_type in DATA_TYPES:
            for item_count in [SMALL_SET, LARGE_SET]:
                for max_size in [SMALL_CACHE, LARGE_CACHE]:
                    for backend in [
                        None,
                    ]:
                        for lru in [True, False]:
                            for in_cache in [True, False]:
                                if not item_count or not max_size:
                                    continue
                                if in_cache and max_size < LARGE_CACHE:
                                    continue
                                yield (
                                    item_count,
                                    lru,
                                    max_size,
                                    backend,
                                    in_cache,
                                    data_type,
                                    fd,
                                )


def run_and_summarize_tests(  # pylint: disable=too-many-arguments
    pydict,
    dcd,
    test_keys,
    logfd,
    item_count,
    lru,
    max_size,
    backend,
    in_cache,
    data_type,
):
    """Run all tests and summarize the results."""
    tests_to_run = [
        ("Rand Access", random_access),
        ("Rand Update", random_update),
        ("Sequential Access", sequential_access),
        ("Sequential Update", sequential_update),
        ("Iterate", iterate),
        ("Random Actions", random_actions),
    ]

    for test_name, test_func in tests_to_run:
        py_times, dcd_times = [], []
        for _ in range(NUM_RUNS):
            py_times.append(test_func(pydict, test_keys))
            dcd_times.append(test_func(dcd, test_keys))

        log(
            test_name,
            statistics.mean(py_times),
            statistics.stdev(py_times) if len(py_times) > 1 else 0,
            statistics.mean(dcd_times),
            statistics.stdev(dcd_times) if len(dcd_times) > 1 else 0,
            log_fd=logfd,
            item_count=item_count,
            lru=lru,
            max_size=max_size,
            backend=backend,
            in_cache=in_cache,
            data_type=data_type,
        )


def run_test_config(  # pylint: disable=too-many-locals,too-many-arguments
    item_count, lru, max_size, backend, in_cache, data_type, logfd
):
    """Run a single performance test configuration."""
    keys, items = DATA_TYPES[data_type](item_count)
    print("-" * 80)
    print(
        f"Running: lru={lru}, max_size={max_size}, backend={backend or 'default'}, "
        f"item_count={item_count}, in_cache_only={in_cache}, data_type={data_type}"
    )
    print("-" * 80)

    with temp_db() as db_file:
        pydict = collections.defaultdict(list)
        dcd = disk_cache_dict.DefaultCacheDict(
            default_factory=list,
            max_size=max_size,
            db_file=db_file,
            lru=lru,
            current_dictionary=None,
            backend=backend,
        )

        # Populate is a special case, so we run it once
        py_time = populate(pydict, items)
        dcd_time = populate(dcd, items)
        log("Populate", py_time, 0, dcd_time, 0, data_type=data_type)

        if in_cache:
            # pylint: disable=protected-access
            test_keys = list(dcd._cache.keys())
        else:
            test_keys = keys

        run_and_summarize_tests(
            pydict,
            dcd,
            test_keys,
            logfd,
            item_count,
            lru,
            max_size,
            backend,
            in_cache,
            data_type,
        )


def visualize_results(log_file="perflog.csv"):
    """Generate plots from the performance log file."""
    # pylint: disable=import-outside-toplevel
    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError:
        print(
            "\n---"
            "\nPandas and Matplotlib are required for visualization."
            "\nPlease install them: pip install pandas matplotlib"
            "\n---"
        )
        return

    df = pd.read_csv(log_file)

    # Plot 1: Overall Performance Comparison by Data Type
    fig, ax = plt.subplots(figsize=(12, 7))
    for data_type, group in df.groupby("data_type"):
        ax.bar(
            group["name"],
            group["dcd_mean"],
            yerr=group["dcd_std"],
            label=f"DCD ({data_type})",
            alpha=0.7,
        )
    ax.set_title("Overall Performance Comparison by Data Type")
    ax.set_xlabel("Test Name")
    ax.set_ylabel("Mean Time (s)")
    ax.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("performance_comparison_by_datatype.png")
    plt.close(fig)

    # Plot 2: LRU Cache Impact
    fig, ax = plt.subplots(figsize=(12, 7))
    for lru, group in df.groupby("lru"):
        ax.bar(
            group["name"],
            group["dcd_mean"],
            yerr=group["dcd_std"],
            label=f"LRU: {lru}",
            alpha=0.7,
        )
    ax.set_title("LRU Cache Impact on Performance")
    ax.set_xlabel("Test Name")
    ax.set_ylabel("Mean Time (s)")
    ax.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("lru_impact.png")
    plt.close(fig)

    # Plot 3: Cache Size Impact
    fig, ax = plt.subplots(figsize=(12, 7))
    for max_size, group in df.groupby("max_size"):
        ax.bar(
            group["name"],
            group["dcd_mean"],
            yerr=group["dcd_std"],
            label=f"Max Size: {max_size}",
            alpha=0.7,
        )
    ax.set_title("Cache Size Impact on Performance")
    ax.set_xlabel("Test Name")
    ax.set_ylabel("Mean Time (s)")
    ax.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("cache_size_impact.png")
    plt.close(fig)

    print("\nVisualizations saved to PNG files.")


def main():
    """Run performance tests on disk cache dict."""
    log_file = "perflog.csv"
    for (
        item_count,
        lru,
        max_size,
        backend,
        in_cache,
        data_type,
        logfd,
    ) in gen_tests():
        run_test_config(item_count, lru, max_size, backend, in_cache, data_type, logfd)

    visualize_results(log_file)


if __name__ == "__main__":
    main()
