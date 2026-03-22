import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def _count_in_file(path_query: tuple[str, bytes]) -> tuple[int, int, float, float]:
    """Read one file and count query occurrences.

    Returns (count, bytes_read, t_read, t_count) where t_read and t_count are
    the per-file wall-clock durations for the disk read and the count scan.
    """
    path, query = path_query

    t0 = time.perf_counter()
    with open(path, "rb") as f:
        data = f.read()
    t_read = time.perf_counter() - t0

    t0 = time.perf_counter()
    count = data.count(query)
    t_count = time.perf_counter() - t0

    return count, len(data), t_read, t_count


def scan_files(directory: str | Path, query: str) -> dict:
    """Count total occurrences of *query* across all files in *directory*.

    Returns a dict with timing, counts, and throughput metrics.
    """
    t_total_start = time.perf_counter()
    query_bytes = query.encode()

    # ── Phase 1: directory listing ────────────────────────────────────────────
    t0 = time.perf_counter()
    entries = [(e.path, query_bytes) for e in os.scandir(directory) if e.is_file()]
    t_scan = time.perf_counter() - t0
    file_count = len(entries)

    # ── Phase 2: parallel read + count ────────────────────────────────────────
    t0 = time.perf_counter()
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(_count_in_file, entries))
    t_parallel = time.perf_counter() - t0

    # ── Phase 3: aggregate ────────────────────────────────────────────────────
    t0 = time.perf_counter()
    total_count = sum(c for c, _, __, ___ in results)
    total_bytes = sum(b for _, b, __, ___ in results)
    worker_read = sum(t_r for _, __, t_r, _ in results)
    worker_count = sum(t_c for _, __, ___, t_c in results)
    t_agg = time.perf_counter() - t0

    t_total = time.perf_counter() - t_total_start

    return {
        "file_count": file_count,
        "total_count": total_count,
        "total_bytes": total_bytes,
        "t_scan": t_scan,
        "t_parallel": t_parallel,
        "worker_read": worker_read,
        "worker_count": worker_count,
        "t_agg": t_agg,
        "t_total": t_total,
    }


def main(directory):
    query = "9"

    print(f"\nScanning '{directory}' for {query!r}\n")
    print("─" * 52)

    m = scan_files(directory, query)

    total_mb = m["total_bytes"] / 1_048_576
    total_gb = m["total_bytes"] / 1_073_741_824
    disk_mb_s = total_mb / m["t_parallel"]
    disk_gb_s = total_gb / m["t_parallel"]

    # Worker-time share: how each op consumed worker seconds across all threads
    worker_total = m["worker_read"] + m["worker_count"]
    pct_read = 100 * m["worker_read"] / worker_total
    pct_count = 100 * m["worker_count"] / worker_total

    print(f"Phase breakdown")
    print(f"  Directory scan  : {m['t_scan'] * 1000:>9.1f} ms")
    print(f"  Parallel block  : {m['t_parallel']:>9.3f} s   (wall-clock)")
    print(f"    ├─ disk read  : {m['worker_read']:>9.3f} s   ({pct_read:.1f}% of worker time)")
    print(f"    └─ count scan : {m['worker_count'] * 1000:>9.1f} ms  ({pct_count:.1f}% of worker time)")
    print(f"  Aggregation     : {m['t_agg'] * 1000:>9.3f} ms")
    print(f"  {'─' * 34}")
    print(f"  Total           : {m['t_total']:>9.3f} s")
    print()
    print(f"Results")
    print(f"  Files scanned   : {m['file_count']:>12,}")
    print(f"  Occurrences     : {m['total_count']:>12,}")
    print(f"  Data read       : {total_gb:>11.2f} GB  ({total_mb:,.0f} MB)")
    print()
    print(f"Throughput")
    print(f"  Disk read speed : {disk_gb_s:>9.3f} GB/s  ({disk_mb_s:,.1f} MB/s)")
    print(f"  Files/s         : {m['file_count'] / m['t_total']:>9,.0f} files/s")
    print()


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    directory = root / "test_files" / "small_files"
    main(directory)
    directory = root / "test_files" / "large_files"
    main(directory)
