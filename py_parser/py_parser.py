import os
import re
import time
import logging
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _has_real_handler() -> bool:
    """Return True if a non-NullHandler is reachable in the logger hierarchy."""
    log: logging.Logger | None = logger
    while log is not None:
        if any(not isinstance(h, logging.NullHandler) for h in log.handlers):
            return True
        if not log.propagate:
            break
        log = log.parent
    return False


def _emit(msg: str) -> None:
    """Send *msg* via logger.debug() when logging is configured, else print()."""
    if _has_real_handler():
        logger.debug(msg)
    else:
        print(msg)


def _count_in_file(args: tuple[str, str]) -> tuple[int, int]:
    """Read one file and count regex pattern matches.

    Returns (count, bytes_read).
    """
    path, pattern = args
    with open(path, "rb") as f:
        data = f.read()
    compiled = re.compile(pattern.encode())
    return len(compiled.findall(data)), len(data)


def scan_files(
    directory: str | Path,
    pattern: str,
    recursive: bool = True,
    print_stats: bool = False,
) -> list[Path]:
    """Count total regex matches of *pattern* across all files in *directory*.

    Args:
        directory: Path to scan for files.
        pattern: Regex pattern to search for in each file.
        print_stats: If True, print timing and throughput stats.
        recursive: If True, descend into subdirectories.
    Returns:
        List of file paths that matched the pattern at least once.
    """
    t_total_start = time.perf_counter()

    if recursive:
        entries = [(str(p), pattern) for p in Path(directory).rglob("*") if p.is_file()]
    else:
        entries = [(e.path, pattern) for e in os.scandir(directory) if e.is_file()]
    file_count = len(entries)

    num_workers = min(os.cpu_count() or 4, 61)
    chunksize = max(1, file_count // (num_workers * 4))

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(_count_in_file, entries, chunksize=chunksize))

    total_count = sum(c for c, _ in results)
    total_bytes = sum(b for _, b in results)

    t_total = time.perf_counter() - t_total_start

    if print_stats:
        total_mb = total_bytes / 1_048_576
        total_gb = total_bytes / 1_073_741_824

        _emit(f"  Total time........{t_total:3f}s")
        _emit(f"  Files scanned.....{file_count:,}")
        _emit(f"  Occurrences.......{total_count:,}")
        _emit(f"  Data read.........{total_gb:.2f} GB  ({total_mb:,.0f} MB)")
        _emit(f"  Throughput........{total_gb / t_total:.3f} GB/s  ({total_mb / t_total:,.1f} MB/s)")
        _emit(f"  Files/s...........{file_count / t_total:,.0f} files/s")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
    pattern = r"9.9"

    root = Path(__file__).parent.parent
    directory = root / "test_files" / "small_files"

    print(f"\nScanning '{directory}' for {pattern!r}\n")
    print("─" * 52)

    m = scan_files(directory, pattern, True)

    directory = root / "test_files" / "large_files"

    print(f"\nScanning '{directory}' for {pattern!r}\n")
    print("─" * 52)

    m = scan_files(directory, pattern, True)
