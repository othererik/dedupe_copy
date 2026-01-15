
import sys
import os
import time
import tracemalloc
import hashlib
from unittest.mock import MagicMock
import types

# Mock rich dependency to allow importing dedupe_copy.utils
rich = types.ModuleType("rich")
rich.console = MagicMock()
rich.progress = MagicMock()
rich.logging = MagicMock()
rich.theme = MagicMock()
sys.modules["rich"] = rich
sys.modules["rich.console"] = rich.console
sys.modules["rich.progress"] = rich.progress
sys.modules["rich.logging"] = rich.logging
sys.modules["rich.theme"] = rich.theme

try:
    from dedupe_copy.utils import hash_file
except ImportError:
    # Fallback if import still fails
    print("Failed to import dedupe_copy.utils even with mocks.")
    sys.exit(1)

def benchmark(size_mb=100, iterations=5):
    filename = "test_large_file.bin"
    # Create a large file
    with open(filename, "wb") as f:
        f.write(os.urandom(size_mb * 1024 * 1024))

    print(f"Benchmarking hash_file (actual implementation) with {size_mb}MB file over {iterations} iterations...")

    times = []
    peak_memories = []

    for _ in range(iterations):
        tracemalloc.start()
        start_time = time.time()

        hash_file(filename, hash_algo="md5")

        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(end_time - start_time)
        peak_memories.append(peak / 1024 / 1024) # MB

    avg_time = sum(times) / len(times)
    avg_peak = sum(peak_memories) / len(peak_memories)

    print(f"Average Time: {avg_time:.4f} seconds")
    print(f"Average Peak Memory Overhead: {avg_peak:.4f} MB")

    os.remove(filename)

if __name__ == "__main__":
    benchmark()
