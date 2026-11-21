#!/usr/bin/env python3
"""Interleaved A/B wall-clock timing of shopping-sessions builds.

Usage: ab.py DATA_DIR RUNS BIN_A BIN_B [BIN_C ...]

Every round runs each binary once, in order, so machine drift hits all of them equally. Prints each
binary's median wall time and its median per-round difference from the first. The times include
about 10 ms of Python process start, the same for every binary.
"""
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

data, runs, bins = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
out = tempfile.mkdtemp()


def run(binary):
    """Returns (wall ms, CPU ms of the child: user + system)."""
    shutil.rmtree(out, ignore_errors=True)
    start = time.perf_counter()
    process = subprocess.Popen([binary, data, out], stdout=subprocess.DEVNULL)
    _, status, usage = os.wait4(process.pid, 0)
    wall = (time.perf_counter() - start) * 1000
    if status != 0:
        sys.exit(f"{binary} failed with status {status}")
    return wall, (usage.ru_utime + usage.ru_stime) * 1000


for binary in bins:  # warm-up
    run(binary)
times = {binary: [] for binary in bins}
for _ in range(runs):
    for binary in bins:
        times[binary].append(run(binary))
shutil.rmtree(out, ignore_errors=True)
for binary in bins:
    wall = [w for w, _ in times[binary]]
    cpu = [c for _, c in times[binary]]
    base_wall = [w for w, _ in times[bins[0]]]
    delta = [a - b for a, b in zip(wall, base_wall)]
    print(f"{binary}: wall median {statistics.median(wall):.1f} ms "
          f"(vs first {statistics.median(delta):+.1f}, min {min(wall):.0f}, "
          f"IQR {statistics.quantiles(wall, n=4)[2] - statistics.quantiles(wall, n=4)[0]:.1f}), "
          f"cpu median {statistics.median(cpu):.0f} ms")
