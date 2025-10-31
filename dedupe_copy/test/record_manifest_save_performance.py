"""
Measures and plots the time it takes to save a Manifest file with a
varying number of file paths stored.
"""

import argparse
import os
import random
import string
import time
from pathlib import Path
import tempfile

try:
    import matplotlib.pyplot as plt

    # Use more readable labels for log scale
    from matplotlib.ticker import FuncFormatter
except ImportError:
    plt = None  # type: ignore

from dedupe_copy.manifest import Manifest


TEST_SCALES = [100, 1000, 10000, 100000, 250000, 500000, 750000, 1000000]


def generate_fake_data(item_count):
    """
    Generates a dictionary of fake hash-to-filepaths data.

    Args:
        item_count (int): The number of items to generate.

    Returns:
        dict: A dictionary mapping fake hashes to lists of fake file paths.
    """
    data = {}
    for i in range(item_count):
        # Generate a fake 32-char hash
        fake_hash = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=32)
        )
        # Generate a fake file path
        fake_path = f"/tmp/fake/path/file_{i}.txt"
        data[fake_hash] = [fake_path]
    return data


def main(output_dir):
    """
    Runs the performance test and generates the plot.

    Args:
        output_dir (str): The directory to save the performance chart in.
    """
    if not plt:
        print(
            "Matplotlib is not installed. Please install it to generate the plot:"
            "\n  pip install .[plot]"
        )
        return

    print("Starting manifest save performance test...")
    save_times = []

    for scale in TEST_SCALES:
        print(f"Testing scale: {scale:,} items...")
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = os.path.join(temp_dir, "perf_manifest.db")
            manifest = Manifest(
                manifest_paths=None, save_path=manifest_path, temp_directory=temp_dir
            )

            fake_data = generate_fake_data(scale)
            manifest.md5_data.update(fake_data)

            start_time = time.perf_counter()
            manifest.save()
            end_time = time.perf_counter()

            duration = end_time - start_time
            save_times.append(duration)
            print(f"  Save time: {duration:.4f} seconds")

    # Generate and save the plot
    _, ax = plt.subplots(figsize=(10, 6))
    ax.plot(TEST_SCALES, save_times, marker="o")

    ax.set_title("Manifest Save Performance")
    ax.set_xlabel("Number of Files in Manifest")
    ax.set_ylabel("Time to Save (seconds)")
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    ax.set_xscale("log")
    ax.set_yscale("log")

    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f"{y:.2f}"))

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    chart_file = output_path / "manifest_save_performance.png"

    plt.tight_layout()
    plt.savefig(chart_file)

    print(f"\nPerformance chart saved to: {chart_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run manifest save performance test and generate a chart."
    )
    parser.add_argument(
        "--output-dir",
        default="dedupe_copy/test/performance_data",
        help="The directory to save the performance chart.",
    )
    args = parser.parse_args()
    main(args.output_dir)
