"""
Runs both file generators sequentially:
  1. generate_small_files  -> 100,000 files x 1,000 lines
  2. generate_large_files  ->   1,000 files x 100,000 lines

Usage:
  python generate_all_files.py             # resume — skip valid files
  python generate_all_files.py -f          # wipe and regenerate everything
  python generate_all_files.py --workers 16
"""

import argparse
import time

import generate_small_files
import generate_large_files

DEFAULT_WORKERS = 32


def main(force: bool = False, workers: int = DEFAULT_WORKERS) -> None:
    total_start = time.perf_counter()

    print("=" * 60)
    print("STEP 1 / 2 — Small files (100k x 1k lines)")
    print("=" * 60)
    generate_small_files.main(force=force, workers=workers)

    print()
    print("=" * 60)
    print("STEP 2 / 2 — Large files (1k x 100k lines)")
    print("=" * 60)
    generate_large_files.main(force=force, workers=workers)

    elapsed = time.perf_counter() - total_start
    mins, secs = divmod(int(elapsed), 60)
    print()
    print("=" * 60)
    print(f"All done in {mins}m {secs}s")
    print("=" * 60)


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