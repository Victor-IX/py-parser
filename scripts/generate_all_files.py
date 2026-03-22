"""
Runs both file generators sequentially:
  1. generate_small_files  → 100,000 files x 1,000 lines
  2. generate_large_files  → 1,000 files   x 100,000 lines
"""

import time

import generate_small_files
import generate_large_files


def main() -> None:
    total_start = time.perf_counter()

    print("=" * 60)
    print("STEP 1 / 2 — Small files")
    print("=" * 60)
    generate_small_files.main()

    print()
    print("=" * 60)
    print("STEP 2 / 2 — Large files")
    print("=" * 60)
    generate_large_files.main()

    elapsed = time.perf_counter() - total_start
    mins, secs = divmod(int(elapsed), 60)
    print()
    print("=" * 60)
    print(f"All done in {mins}m {secs}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
