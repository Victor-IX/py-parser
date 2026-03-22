"""
Generates 100,000 small .txt files in test_files/small_files/.
Each file has 1,000 lines with 100 random characters per line.
"""

import random
import string
import time
from pathlib import Path

OUTPUT_DIR = Path("test_files") / "small_files"
NUM_FILES = 100_000
NUM_LINES = 1_000
LINE_LENGTH = 100
CHARS = string.ascii_letters + string.digits

REPORT_INTERVAL = 1_000


def generate_file_content() -> bytes:
    lines = ("".join(random.choices(CHARS, k=LINE_LENGTH)) + "\n" for _ in range(NUM_LINES))
    return "".join(lines).encode()


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Generating {NUM_FILES:,} files → {OUTPUT_DIR}")
    print(f"Each file: {NUM_LINES:,} lines x {LINE_LENGTH} chars  (~{NUM_LINES * (LINE_LENGTH + 1) / 1024:.0f} KB)")
    print(f"Estimated total: ~{NUM_FILES * NUM_LINES * (LINE_LENGTH + 1) / 1024 / 1024 / 1024:.1f} GB\n")

    start = time.perf_counter()

    for i in range(NUM_FILES):
        path = OUTPUT_DIR / f"file_{i:06d}.txt"
        with open(path, "wb") as f:
            f.write(generate_file_content())

        if (i + 1) % REPORT_INTERVAL == 0:
            elapsed = time.perf_counter() - start
            rate = (i + 1) / elapsed
            remaining = (NUM_FILES - i - 1) / rate
            print(
                f"\r  {i + 1:>7,} / {NUM_FILES:,}  ({rate:.0f} files/s  ETA {remaining:.0f}s)   ",
                end="",
                flush=True,
            )

    elapsed = time.perf_counter() - start
    print(f"\n\nDone. {NUM_FILES:,} files written in {elapsed:.1f}s ({NUM_FILES / elapsed:.0f} files/s)")


if __name__ == "__main__":
    main()
