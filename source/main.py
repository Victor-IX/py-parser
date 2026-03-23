import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path


def _count_in_file(path_query: tuple[str, bytes]) -> tuple[int, int]:
    """Read one file and count query occurrences.

    Returns (count, bytes_read).
    """
    path, query = path_query
    with open(path, "rb") as f:
        data = f.read()
    return data.count(query), len(data)


def scan_files(directory: str | Path, query: str) -> dict:
    """Count total occurrences of *query* across all files in *directory*.

    Uses ProcessPoolExecutor to bypass the GIL — each worker process runs its
    own interpreter, so file I/O and byte counting truly execute in parallel.

    Returns a dict with timing, counts, and throughput metrics.
    """
    t_total_start = time.perf_counter()
    query_bytes = query.encode()

    entries = [(e.path, query_bytes) for e in os.scandir(directory) if e.is_file()]
    file_count = len(entries)

    num_workers = min(os.cpu_count() or 4, 61)
    chunksize = max(1, file_count // (num_workers * 4))

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(_count_in_file, entries, chunksize=chunksize))

    total_count = sum(c for c, _ in results)
    total_bytes = sum(b for _, b in results)

    t_total = time.perf_counter() - t_total_start

    return {
        "file_count": file_count,
        "total_count": total_count,
        "total_bytes": total_bytes,
        "t_total": t_total,
    }


def main(directory):
    query = "9"

    print(f"\nScanning '{directory}' for {query!r}\n")
    print("─" * 52)

    m = scan_files(directory, query)

    total_mb = m["total_bytes"] / 1_048_576
    total_gb = m["total_bytes"] / 1_073_741_824

    print(f"  Total time      : {m['t_total']:>9.3f} s")
    print(f"  Files scanned   : {m['file_count']:>12,}")
    print(f"  Occurrences     : {m['total_count']:>12,}")
    print(f"  Data read       : {total_gb:>11.2f} GB  ({total_mb:,.0f} MB)")
    print(f"  Throughput      : {total_gb / m['t_total']:>9.3f} GB/s  ({total_mb / m['t_total']:,.1f} MB/s)")
    print(f"  Files/s         : {m['file_count'] / m['t_total']:>9,.0f} files/s")
    print()


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    directory = root / "test_files" / "small_files"
    main(directory)
    directory = root / "test_files" / "large_files"
    main(directory)
