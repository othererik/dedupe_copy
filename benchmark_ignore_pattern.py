import fnmatch
import time
import re
import random
import string
import os
from typing import List, Optional

# Mock WalkConfig to simulate the real environment
class WalkConfig:
    def __init__(self, ignore=None):
        self.ignore = ignore
        self.ignore_regex = None
        if self.ignore:
            normalized_patterns = [os.path.normcase(p) for p in self.ignore]
            regex_patterns = [fnmatch.translate(p) for p in normalized_patterns]
            combined_pattern = "|".join(regex_patterns)
            self.ignore_regex = re.compile(combined_pattern)

# --- Original Implementation ---
def original_check(
    filepath: str,
    ignore: Optional[List[str]],
) -> bool:
    if ignore:
        for ignored_pattern in ignore:
            if fnmatch.fnmatch(filepath, ignored_pattern):
                return False
    return True

# --- Optimized Implementation (Mimicking _is_file_processing_required) ---
def optimized_check(
    filepath: str,
    ignore: Optional[List[str]],
    ignore_regex: Optional[re.Pattern] = None,
) -> bool:
    if ignore_regex:
        if ignore_regex.match(os.path.normcase(filepath)):
            # In the real code we loop to find the specific pattern for logging
            # Here we just return False as per the performance critical path
            return False
    elif ignore:
        for ignored_pattern in ignore:
            if fnmatch.fnmatch(filepath, ignored_pattern):
                return False
    return True

# --- Setup Benchmark ---

def generate_random_path(depth=3):
    parts = []
    for _ in range(depth):
        parts.append(''.join(random.choices(string.ascii_lowercase, k=8)))
    filename = ''.join(random.choices(string.ascii_lowercase, k=10)) + ".txt"
    return "/".join(parts) + "/" + filename

def benchmark():
    num_files = 100000
    num_patterns = 50

    # Generate random paths
    paths = [generate_random_path() for _ in range(num_files)]

    # Generate ignore patterns
    ignore_patterns = [
        "*.log", "*.tmp", "*.bak", "*/temp/*", "*/cache/*",
        "*.pyc", "*.o", "*.obj", ".git/*", ".svn/*",
        "node_modules/*", "venv/*", "dist/*", "build/*",
        "test_results/*", "*.test", "*.spec",
        # Add some random ones to increase N
        *[f"*.{c*3}" for c in string.ascii_lowercase]
    ]

    # Ensure some paths match
    paths.append("/some/path/test.log")
    paths.append("/project/venv/bin/python")

    print(f"Benchmarking with {num_files} files and {len(ignore_patterns)} ignore patterns.")

    # Measure Original
    start_time = time.time()
    matches_original = 0
    for path in paths:
        if not original_check(path, ignore_patterns):
            matches_original += 1
    end_time = time.time()
    original_duration = end_time - start_time
    print(f"Original Implementation: {original_duration:.4f} seconds (Ignored: {matches_original})")

    # Measure Optimized
    start_time = time.time()
    config = WalkConfig(ignore=ignore_patterns) # Compilation happens here

    compilation_start = time.time()
    _ = WalkConfig(ignore=ignore_patterns)
    compilation_time = time.time() - compilation_start
    print(f"Regex Compilation Time: {compilation_time:.4f} seconds")

    start_time = time.time()
    matches_optimized = 0
    for path in paths:
        if not optimized_check(path, ignore_patterns, config.ignore_regex):
            matches_optimized += 1
    end_time = time.time()
    optimized_duration = end_time - start_time
    print(f"Optimized Implementation: {optimized_duration:.4f} seconds (Ignored: {matches_optimized})")

    if optimized_duration > 0:
        print(f"Speedup: {original_duration / optimized_duration:.2f}x")
    else:
        print("Speedup: Infinite (too fast to measure)")

    assert matches_original == matches_optimized

if __name__ == "__main__":
    benchmark()
