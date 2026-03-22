"""
Generates 1,000 large .txt files in test_files/large_files/.
Each file has 100,000 lines with 100 random characters per line.
"""

import random
import string
import time
from pathlib import Path

OUTPUT_DIR = Path("test_files") / "large_files"
NUM_FILES = 1_000
NUM_LINES = 100_000
LINE_LENGTH = 100
CHARS = string.ascii_letters + string.digits

CHUNK_SIZE = 1_000  # lines written per chunk to keep memory usage low


def write_large_file(path: Path) -> None:
    with open(path, "wb") as f:
        for chunk_start in range(0, NUM_LINES, CHUNK_SIZE):
            count = min(CHUNK_SIZE, NUM_LINES - chunk_start)
            chunk = "".join("".join(random.choices(CHARS, k=LINE_LENGTH)) + "\n" for _ in range(count))
            f.write(chunk.encode())


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Generating {NUM_FILES:,} files → {OUTPUT_DIR}")
    print(
        f"Each file: {NUM_LINES:,} lines x {LINE_LENGTH} chars  (~{NUM_LINES * (LINE_LENGTH + 1) / 1024 / 1024:.1f} MB)"
    )
    print(f"Estimated total: ~{NUM_FILES * NUM_LINES * (LINE_LENGTH + 1) / 1024 / 1024 / 1024:.1f} GB\n")

    start = time.perf_counter()

    for i in range(NUM_FILES):
        path = OUTPUT_DIR / f"file_{i:04d}.txt"
        write_large_file(path)

        elapsed = time.perf_counter() - start
        rate = (i + 1) / elapsed
        remaining = (NUM_FILES - i - 1) / rate
        print(
            f"\r  {i + 1:>4} / {NUM_FILES}  ({rate:.1f} files/s  ETA {remaining:.0f}s)   ",
            end="",
            flush=True,
        )

    elapsed = time.perf_counter() - start
    print(f"\n\nDone. {NUM_FILES:,} files written in {elapsed:.1f}s ({NUM_FILES / elapsed:.1f} files/s)")


if __name__ == "__main__":
    main()
