import os
import re
import time
import logging
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_BINARY_CHECK_BYTES = 8192  # bytes sampled for null-byte binary detection


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


def _normalize_extensions(extensions: str | Iterable[str] | None) -> frozenset[str] | None:
    """Return a frozenset of lowercase dot-prefixed extensions, or None for no filter."""
    if extensions is None:
        return None
    if isinstance(extensions, str):
        extensions = (extensions,)
    return frozenset(ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in extensions)


def _is_literal_pattern(pattern: str) -> bool:
    """Return True if *pattern* contains no regex metacharacters."""
    return not re.search(r"[\\.*+?^${}()|[\]]", pattern)


def _search_file(args: tuple[str, str, bool]) -> tuple[bool, int]:
    """Read one file and check for a pattern match (early exit).

    Returns (matched, bytes_read).  matched is None when the file was
    identified as binary and skipped.
    """
    path, pattern, skip_binary = args
    with open(path, "rb") as f:
        if skip_binary:
            header = f.read(_BINARY_CHECK_BYTES)
            if b"\x00" in header:
                return None, 0
            data = header + f.read()
        else:
            data = f.read()
    pattern_bytes = pattern.encode()
    if _is_literal_pattern(pattern):
        matched = pattern_bytes in data
    else:
        matched = re.search(pattern_bytes, data) is not None
    return matched, len(data)


class FileScanner:
    """Scans files in a directory for regex or literal pattern matches.

    Args:
        directory:    Root directory to scan.
        recursive:    If True, descend into subdirectories.
        extensions:   Whitelist of file extensions to scan, e.g. ``".txt"`` or
                      ``[".txt", ".log"]``.  Leading dot and case are normalised
                      automatically.  ``None`` (default) scans all file types.
        skip_binary:  If True, binary files are skipped when no extension filter
                      is set.
    """

    def __init__(
        self,
        directory: str | Path,
        recursive: bool = True,
        extensions: str | Iterable[str] | None = None,
        skip_binary: bool = True,
    ) -> None:
        self.directory = str(directory)
        self.recursive = recursive
        self.skip_binary = skip_binary

        self._ext_filter = _normalize_extensions(extensions)
        self._ext_tuple = tuple(self._ext_filter) if self._ext_filter is not None else None
        self._effective_skip = skip_binary and self._ext_filter is None

    def _discover_files(self) -> list[str]:
        """Return file paths under the directory, filtered by extension."""
        if self.recursive:
            paths: list[str] = []
            stack = [self.directory]
            while stack:
                current = stack.pop()
                try:
                    with os.scandir(current) as it:
                        for entry in it:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                            elif entry.is_file(follow_symlinks=False):
                                if self._ext_tuple is None or entry.name.lower().endswith(self._ext_tuple):
                                    paths.append(entry.path)
                except PermissionError:
                    pass
            return paths

        return [
            e.path
            for e in os.scandir(self.directory)
            if e.is_file(follow_symlinks=False)
            and (self._ext_tuple is None or e.name.lower().endswith(self._ext_tuple))
        ]

    def scan(self, pattern: str, *, print_stats: bool = False) -> list[Path]:
        """Search *pattern* across files and return matching paths.

        Args:
            pattern:      Regex pattern (or literal string) to search for.
            print_stats:  If True, emit timing and throughput stats.
        """
        t_total_start = time.perf_counter()

        str_paths = self._discover_files()

        entries = [(p, pattern, self._effective_skip) for p in str_paths]
        file_count = len(entries)

        num_workers = min(os.cpu_count() or 4, 61)
        chunksize = max(1, file_count // (num_workers * 4))

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(_search_file, entries, chunksize=chunksize))

        matched = [Path(p) for p, (hit, _) in zip(str_paths, results) if hit]
        skipped_binary = sum(1 for hit, _ in results if hit is None)

        scanned_results = [(h, b) for h, b in results if h is not None]
        total_bytes = sum(b for _, b in scanned_results)
        t_total = time.perf_counter() - t_total_start

        if print_stats:
            total_mb = total_bytes / 1_048_576
            total_gb = total_bytes / 1_073_741_824

            _emit(f"  Total time........{t_total:.3f}s")
            _emit(f"  Files scanned.....{file_count - skipped_binary:,}")
            if skipped_binary:
                _emit(f"  Binary skipped....{skipped_binary:,}")
            _emit(f"  Files matched.....{len(matched):,}")
            _emit(f"  Data read.........{total_gb:.2f} GB  ({total_mb:,.0f} MB)")
            _emit(f"  Throughput........{total_gb / t_total:.3f} GB/s  ({total_mb / t_total:,.1f} MB/s)")
            _emit(f"  Files/s...........{(file_count - skipped_binary) / t_total:,.0f} files/s")

        return matched


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
    pattern = r"omc_utility_heavy_helmet"

    root = Path(__file__).parent.parent

    for name, kwargs in [
        ("small_files", {"directory": root / "test_files" / "small_files", "skip_binary": False}),
        ("large_files", {"directory": root / "test_files" / "large_files", "skip_binary": False}),
        (
            "perforce",
            {
                "directory": r"D:\perforce\vchedeville_sc-patch-review",
                "extensions": [".xml", ".mtl", ".cdf", ".skin", ".ma"],
            },
        ),
    ]:
        scanner = FileScanner(**kwargs)
        print(f"\nScanning '{kwargs['directory']}' for {pattern!r}\n")
        print("─" * 52)
        m = scanner.scan(pattern, print_stats=True)

    for path in m:
        print(f"  {path}")
