#!/usr/bin/env python3
"""
Project 1: MapReduce for Counting Integer Frequencies
Author: Rishi Guptha Mankala

This script implements two phases:
1. Mapper: Each worker processes an input file, counts integer frequencies,
   and writes results to intermediate JSONs.
2. Reducer: Aggregates all JSONs, merges counts, reports top 6 integers,
   and writes a final analysis report.

Usage:
    python count_integers.py mapper
    python count_integers.py reducer
"""

import os
import sys
import json
import hashlib
from glob import glob
from collections import Counter
from multiprocessing import Pool
from datetime import datetime

# ---------- CONFIGURATION ----------
INPUT_DIR = "/gpfs/projects/AMS598/projects2025_data/project1_data/"
OUTPUT_DIR = "/gpfs/projects/AMS598/class2025/Mankala_RishiGuptha/"
INTERMEDIATE_DIR = os.path.join(OUTPUT_DIR, "intermediate")
MAP_PREFIX = "counts_"
FINAL_JSON = os.path.join(OUTPUT_DIR, "top6_counts.json")
FINAL_REPORT = os.path.join(OUTPUT_DIR, "final_output.txt")
NUM_PROCESSES = 4


# ---------- MAPPER ----------
def mapper_task(file_path: str) -> str:
    """Read file, count integers, dump counts to JSON, return output path."""
    try:
        with open(file_path, "r") as f:
            tokens = f.read().split()
    except FileNotFoundError:
        print(f"File not found: {file_path}", file=sys.stderr)
        return ""

    counts = Counter(int(tok) for tok in tokens if tok.isdigit())

    # Unique filename based on file path hash
    file_hash = hashlib.sha1(file_path.encode()).hexdigest()
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    out_path = os.path.join(INTERMEDIATE_DIR, f"{MAP_PREFIX}{file_hash}.json")

    with open(out_path, "w") as out_f:
        json.dump(counts, out_f)

    return out_path


# ---------- REDUCER ----------
def reducer_task():
    """Aggregate all mapper outputs and save top 6 integers with counts."""
    files = glob(os.path.join(INTERMEDIATE_DIR, f"{MAP_PREFIX}*.json"))
    if not files:
        print("No intermediate files. Run mapper phase first.", file=sys.stderr)
        sys.exit(1)

    total_counts = Counter()
    for fp in files:
        with open(fp, "r") as jf:
            data = json.load(jf)
            total_counts.update({int(k): v for k, v in data.items()})

    top6 = total_counts.most_common(6)

    # Save JSON
    with open(FINAL_JSON, "w") as f:
        json.dump(top6, f, indent=2)

    # Save text report
    write_report(top6, len(files), sum(total_counts.values()))

    print("\u2705 Reducer complete. Top 6 integers:")
    for val, freq in top6:
        print(f"{val}: {freq}")
    print(f"Results saved to {FINAL_JSON} and {FINAL_REPORT}")


# ---------- REPORT ----------
def write_report(top_results, num_files, total_count):
    """Generate a summary report in plain text."""
    with open(FINAL_REPORT, "w") as rep:
        rep.write("Project 1: Counting with MapReduce\n")
        rep.write("=================================\n\n")
        rep.write(f"Date/Time: {datetime.now()}\n")
        rep.write(f"Processed {num_files} mapper output files.\n")
        rep.write(f"Total integers processed: {total_count}\n\n")
        rep.write("Top 6 integers by frequency:\n")
        rep.write("-----------------------------\n")
        for rank, (val, freq) in enumerate(top_results, 1):
            rep.write(f"{rank}. Integer {val} \u2192 {freq} occurrences\n")
        rep.write("\nNotes:\n")
        rep.write("- Mapper phase counts integers per file in parallel (4 workers).\n")
        rep.write("- Reducer merges all counts and selects the 6 most common.\n")
        rep.write("- Intermediate results are JSON, final report is plain text.\n")
        rep.write("- Output files:\n")
        rep.write(f"  * {FINAL_JSON}\n")
        rep.write(f"  * {FINAL_REPORT}\n")


# ---------- MAIN ----------
if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("mapper", "reducer"):
        print("Usage: python count_integers.py [mapper|reducer]", file=sys.stderr)
        sys.exit(1)

    phase = sys.argv[1]
    if phase == "mapper":
        files = sorted(glob(os.path.join(INPUT_DIR, "data*")))
        with Pool(NUM_PROCESSES) as pool:
            outputs = pool.map(mapper_task, files)
        print("Mapper phase finished. Intermediate files created:")
        print("\n".join(fp for fp in outputs if fp))
    else:
        reducer_task()
