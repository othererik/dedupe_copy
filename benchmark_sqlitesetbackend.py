import time
from dedupe_copy.disk_cache_dict import SqliteSetBackend

def benchmark():
    backend = SqliteSetBackend(unlink_old_db=True)
    print("Adding 100,000 items...")
    # Add items
    for i in range(100000):
        backend.add(f"item_{i}")
    backend.commit()

    print("Measuring len()...")
    start_time = time.time()
    for _ in range(1000):
        _ = len(backend)
    end_time = time.time()

    duration = end_time - start_time
    print(f"Time for 1000 len() calls: {duration:.4f} seconds")

if __name__ == "__main__":
    benchmark()
