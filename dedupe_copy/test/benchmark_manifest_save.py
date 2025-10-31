"""
Measures and plots the time it takes to save a Manifest file with a
varying number of file paths stored. Can be run in stages to compare
performance before and after code changes.
"""

import argparse
import os
import random
import string
import time
import json
from pathlib import Path
import tempfile
from typing import Any

try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
except ImportError:
    plt = None  # type: ignore

from dedupe_copy.manifest import Manifest


TEST_SCALES = [100, 1000, 10000, 100000, 250000, 500000, 750000, 1000000]


def generate_fake_data(item_count):
    """
    Generates a dictionary of fake hash-to-filepaths data.
    """
    data = {}
    for i in range(item_count):
        fake_hash = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=32)
        )
        fake_path = f"/tmp/fake/path/file_{i}.txt"
        data[fake_hash] = [[fake_path, i, time.time()]]
    return data


def run_benchmark(run_name, output_dir):
    """
    Runs the performance test for a given configuration and saves the results incrementally.
    """
    print(f"Starting manifest save performance test for '{run_name}'...")
    save_times = []
    scales_completed = []

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / f"performance_results_{run_name}.json"

    for scale in TEST_SCALES:
        print(f"Testing scale: {scale:,} items...")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                manifest_path = os.path.join(temp_dir, "perf_manifest.db")
                manifest = Manifest(
                    manifest_paths=None,
                    save_path=manifest_path,
                    temp_directory=temp_dir,
                )

                fake_data = generate_fake_data(scale)
                # Use direct update to md5_data cache for speed
                # pylint: disable=protected-access
                manifest.md5_data._cache.update(fake_data)

                start_time = time.perf_counter()
                manifest.save()
                end_time = time.perf_counter()

                duration = end_time - start_time
                save_times.append(duration)
                scales_completed.append(scale)
                print(f"  Save time: {duration:.4f} seconds")

                # Save results incrementally
                with open(results_file, "w", encoding="utf-8") as f:
                    json.dump({"scales": scales_completed, "times": save_times}, f)

        # pylint: disable=broad-exception-caught
        except Exception as e:
            print(f"  An error occurred at scale {scale}: {e}")
            print("Stopping benchmark for this run.")
            break

    print(f"\nBenchmark results for '{run_name}' saved to: {results_file}")


def generate_plot(output_dir, run_names):
    """
    Generates a plot comparing the results of different benchmark runs.
    """
    if not plt:
        print(
            "Matplotlib is not installed. Please install it to generate the plot:"
            "\n  pip install .[plot]"
        )
        return

    _, ax = plt.subplots(figsize=(10, 6))

    for name in run_names:
        results_file = Path(output_dir) / f"performance_results_{name}.json"
        if not results_file.exists():
            print(f"Warning: Results file not found for run '{name}'. Skipping.")
            continue
        with open(results_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        ax.plot(data["scales"], data["times"], marker="o", label=name.capitalize())

    ax.set_title("Manifest Save Performance Comparison")
    ax.set_xlabel("Number of Files in Manifest")
    ax.set_ylabel("Time to Save (seconds)")
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend()

    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{y:.2f}"))

    chart_file = Path(output_dir) / "manifest_save_performance_comparison.png"
    plt.tight_layout()
    plt.savefig(chart_file)
    print(f"\nPerformance comparison chart saved to: {chart_file}")


def main():
    """Main function to run benchmark or generate plot."""
    parser = argparse.ArgumentParser(
        description="Run manifest save performance benchmark and generate a comparison chart."
    )
    parser.add_argument(
        "--run-name",
        type=str,
        help="The name for this benchmark run (e.g., 'original', 'optimized').",
    )
    parser.add_argument(
        "--plot",
        nargs="+",
        metavar="RUN_NAME",
        help="Generate a plot comparing the specified runs.",
    )
    parser.add_argument(
        "--output-dir",
        default="dedupe_copy/test/performance_data",
        help="The directory to save benchmark results and the performance chart.",
    )
    args = parser.parse_args()

    if args.run_name:
        run_benchmark(args.run_name, args.output_dir)
    elif args.plot:
        generate_plot(args.output_dir, args.plot)
    else:
        print(
            "Please specify either --run-name to run a benchmark or --plot to generate a chart."
        )


if __name__ == "__main__":
    main()
