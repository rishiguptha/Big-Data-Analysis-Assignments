#!/usr/bin/env python3
import argparse, os, json, glob

def ensure_dirs(work_dir):
    inter = os.path.join(work_dir, "intermediate")
    os.makedirs(inter, exist_ok=True)
    return inter

def list_inputs(data_dir):
    return sorted(glob.glob(os.path.join(data_dir, "*.txt")))

def mapper(data_dir, work_dir, task_index, num_tasks):
    inter = ensure_dirs(work_dir)
    files = list_inputs(data_dir)
    counts = [0]*101
    for i, fp in enumerate(files):
        if i % num_tasks != task_index:
            continue
        with open(fp, "r") as f:
            for line in f:
                for t in line.split():
                    v = int(t)
                    if 0 <= v <= 100:
                        counts[v] += 1
    with open(os.path.join(inter, f"mapper_{task_index}.json"), "w") as w:
        json.dump(counts, w)

def reducer(work_dir, task_index, num_tasks):
    inter = ensure_dirs(work_dir)
    mapper_parts = sorted(glob.glob(os.path.join(inter, "mapper_*.json")))
    partial = [0]*101
    for mp in mapper_parts:
        with open(mp, "r") as r:
            arr = json.load(r)
            for i, c in enumerate(arr):
                if i % num_tasks == task_index:
                    partial[i] += int(c)
    with open(os.path.join(inter, f"reducer_{task_index}.json"), "w") as w:
        json.dump(partial, w)

def report(work_dir, top_k=6):
    inter = ensure_dirs(work_dir)
    reducer_parts = sorted(glob.glob(os.path.join(inter, "reducer_*.json")))
    final = [0]*101
    for rp in reducer_parts:
        with open(rp, "r") as r:
            arr = json.load(r)
            for i, c in enumerate(arr):
                final[i] += int(c)
    pairs = [(i, final[i]) for i in range(101)]
    pairs.sort(key=lambda x: (-x[1], x[0]))
    out_path = os.path.join(work_dir, "output.txt")
    with open(out_path, "w") as w:
        w.write("--- Top 6 Integers by Frequency ---\n")
        w.write("Integer   Frequency\n")
        w.write("-------   ---------\n")
        for i, c in pairs[:top_k]:
            w.write(f"{i:<9}{c}\n")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--phase", required=True, choices=["map","reduce","report"])
    p.add_argument("--data-dir", default="/gpfs/projects/AMS598/projects2025_data/project1_data/")
    p.add_argument("--work-dir", default="/gpfs/projects/AMS598/class2025/rmankala/")
    p.add_argument("--task-index", type=int, default=0)
    p.add_argument("--num-tasks", type=int, default=4)
    args = p.parse_args()
    if args.phase == "map":
        mapper(args.data_dir, args.work_dir, args.task_index, args.num_tasks)
    elif args.phase == "reduce":
        reducer(args.work_dir, args.task_index, args.num_tasks)
    else:
        report(args.work_dir)
