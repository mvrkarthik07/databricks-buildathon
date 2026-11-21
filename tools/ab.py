#!/usr/bin/env python3
"""Interleaved A/B wall-clock timing of shopping-sessions builds.

Usage: ab.py DATA_DIR RUNS BIN_A BIN_B [BIN_C ...]

Every round runs each binary once, in order, so machine drift hits all of them equally. Prints each
binary's median wall time and its median per-round difference from the first. The times include
about 10 ms of Python process start, the same for every binary.
"""
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

data, runs, bins = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
out = tempfile.mkdtemp()


def run(binary):
    shutil.rmtree(out, ignore_errors=True)
    start = time.perf_counter()
    subprocess.run([binary, data, out], stdout=subprocess.DEVNULL, check=True)
    return (time.perf_counter() - start) * 1000


for binary in bins:  # warm-up
    run(binary)
times = {binary: [] for binary in bins}
for _ in range(runs):
    for binary in bins:
        times[binary].append(run(binary))
shutil.rmtree(out, ignore_errors=True)
for binary in bins:
    delta = [a - b for a, b in zip(times[binary], times[bins[0]])]
    print(f"{binary}: median {statistics.median(times[binary]):.1f} ms, "
          f"vs first {statistics.median(delta):+.1f} ms, min {min(times[binary]):.0f} ms")
