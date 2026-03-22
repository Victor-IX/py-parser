"""
Generates 1,000 large .txt files in test_files/large_files/.
Each file has 100,000 lines with 100 random ASCII characters per line.

Usage:
  python generate_large_files.py             # resume — skip valid files
  python generate_large_files.py -f          # wipe and regenerate everything
  python generate_large_files.py --workers 16
"""

import argparse
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

OUTPUT_DIR = Path("test_files") / "large_files"
NUM_FILES = 1_000
NUM_LINES = 100_000
LINE_LENGTH = 100
CHARS = string.ascii_letters + string.digits
EXPECTED_SIZE = NUM_LINES * (LINE_LENGTH + 1)  # LINE_LENGTH chars + \n per line
DEFAULT_WORKERS = 32


def generate_file_content() -> bytes:
    flat = "".join(random.choices(CHARS, k=NUM_LINES * LINE_LENGTH))
    return (
        "\n".join(flat[i : i + LINE_LENGTH] for i in range(0, len(flat), LINE_LENGTH))
        + "\n"
    ).encode()


def write_file(path: Path) -> None:
    path.write_bytes(generate_file_content())


def is_valid(path: Path) -> bool:
    try:
        return path.stat().st_size == EXPECTED_SIZE
    except OSError:
        return False


def build_work_list(force: bool) -> list[Path]:
    if force:
        existing = list(OUTPUT_DIR.glob("*.txt"))
        if existing:
            print(f"  --force: removing {len(existing):,} existing files...")
            for f in existing:
                f.unlink()
        return [OUTPUT_DIR / f"file_{i:04d}.txt" for i in range(NUM_FILES)]

    print("  Scanning existing files for integrity...")
    to_generate: list[Path] = []
    invalid = 0
    for i in range(NUM_FILES):
        path = OUTPUT_DIR / f"file_{i:04d}.txt"
        if not path.exists():
            to_generate.append(path)
        elif not is_valid(path):
            path.unlink()
            to_generate.append(path)
            invalid += 1

    if invalid:
        print(f"  Removed {invalid:,} corrupt file(s) — will regenerate")
    return to_generate


def main(force: bool = False, workers: int = DEFAULT_WORKERS) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    to_generate = build_work_list(force)
    total = len(to_generate)

    if total == 0:
        print(f"  All {NUM_FILES:,} files are valid. Nothing to do.")
        return

    print(
        f"  Generating {total:,} / {NUM_FILES:,} files  "
        f"[{workers} workers  ~{EXPECTED_SIZE / 1024 / 1024:.1f} MB each]"
    )
    start = time.perf_counter()
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(write_file, p): p for p in to_generate}
        for future in as_completed(futures):
            future.result()
            completed += 1
            elapsed = time.perf_counter() - start
            rate = completed / elapsed
            eta = (total - completed) / rate if rate > 0 else 0
            print(
                f"\r  {completed:>4} / {total}  "
                f"({rate:.1f} files/s  ETA {eta:.0f}s)   ",
                end="",
                flush=True,
            )

    elapsed = time.perf_counter() - start
    print(f"\n  Done. {total:,} files in {elapsed:.1f}s  ({total / elapsed:.1f} files/s)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("-f", "--force", action="store_true", help="Delete existing files before generating")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, metavar="N",
                   help=f"Thread count (default: {DEFAULT_WORKERS})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(force=args.force, workers=args.workers)