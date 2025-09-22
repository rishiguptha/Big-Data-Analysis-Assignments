# mapreduce_counter.py
import os
import sys
import glob
from collections import defaultdict
import argparse

# --- Configuration ---
NET_ID = "rmankala" 

# CHANGED: Base directory is now your NetID folder, not a 'project1' subfolder.
BASE_PROJECT_DIR = f"/gpfs/projects/AMS598/class2025/{NET_ID}"

# Directory containing the 16 input data files.
INPUT_DATA_DIR = "/gpfs/projects/AMS598/projects2025_data/project1_data/"

# CHANGED: We only need one directory for all intermediate files.
INTERMEDIATE_DIR = os.path.join(BASE_PROJECT_DIR, "intermediate")

# Number of parallel processes for mappers and reducers.
NUM_MAPPERS = 4
NUM_REDUCERS = 4


def setup_directories():
    """
    Creates the necessary intermediate directory if it doesn't exist.
    """
    # CHANGED: Removed the 'output' directory creation.
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)


def run_mapper(mapper_id):
    """
    Mapper function.
    Reads a subset of input files, counts integer frequencies, and writes
    the local counts to an intermediate file on disk.
    """
    print(f"Mapper {mapper_id}: Starting...")
    
    all_files = sorted(glob.glob(os.path.join(INPUT_DATA_DIR, "*.txt")))
    
    files_per_mapper = len(all_files) // NUM_MAPPERS
    start_index = mapper_id * files_per_mapper
    end_index = start_index + files_per_mapper
    
    if mapper_id == NUM_MAPPERS - 1:
        end_index = len(all_files)

    assigned_files = all_files[start_index:end_index]
    print(f"Mapper {mapper_id}: Assigned {len(assigned_files)} files.")

    local_counts = defaultdict(int)

    for filepath in assigned_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    num = int(line.strip())
                    local_counts[num] += 1
                except (ValueError, TypeError):
                    continue
    
    intermediate_filepath = os.path.join(INTERMEDIATE_DIR, f"map_{mapper_id}.txt")
    with open(intermediate_filepath, 'w') as f:
        for number, count in local_counts.items():
            f.write(f"{number} {count}\n")
            
    print(f"Mapper {mapper_id}: Finished. Intermediate results saved to {intermediate_filepath}")


def run_reducer(reducer_id):
    """
    Reducer function.
    Reads mapper outputs and writes its own aggregated results to the
    SAME intermediate directory.
    """
    print(f"Reducer {reducer_id}: Starting...")
    
    final_counts = defaultdict(int)
    intermediate_files = glob.glob(os.path.join(INTERMEDIATE_DIR, "map_*.txt"))
    
    for filepath in intermediate_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split()
                    if len(parts) == 2:
                        number, count = int(parts[0]), int(parts[1])
                        if number % NUM_REDUCERS == reducer_id:
                            final_counts[number] += count
                except (ValueError, TypeError):
                    continue

    # CHANGED: Reducer now writes its output to the INTERMEDIATE_DIR.
    output_filepath = os.path.join(INTERMEDIATE_DIR, f"reduce_{reducer_id}.txt")
    with open(output_filepath, 'w') as f:
        for number, count in final_counts.items():
            f.write(f"{number} {count}\n")
            
    print(f"Reducer {reducer_id}: Finished. Intermediate counts saved to {output_filepath}")


def run_report():
    """
    Final reporting step.
    Collects results from all reducer files and writes the top 6
    in tabular format to a final output.txt file.
    """
    print("Report Generator: Starting...")
    
    total_counts = {}
    
    # CHANGED: Reads reducer files from the INTERMEDIATE_DIR.
    reducer_files = glob.glob(os.path.join(INTERMEDIATE_DIR, "reduce_*.txt"))

    if not reducer_files:
        print("Error: No reducer output files found. Did the reducer step run correctly?")
        return

    for filepath in reducer_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split()
                    if len(parts) == 2:
                        number, count = int(parts[0]), int(parts[1])
                        total_counts[number] = count
                except (ValueError, TypeError):
                    continue
                    
    sorted_counts = sorted(total_counts.items(), key=lambda item: item[1], reverse=True)
    
    # CHANGED: Defines the final output file path.
    final_output_path = os.path.join(BASE_PROJECT_DIR, "output.txt")
    
    # CHANGED: Writes the report to the output.txt file instead of printing to the log.
    with open(final_output_path, 'w') as f:
        f.write("--- Final Report: Top 6 Integers by Frequency ---\n")
        f.write("Integer\t\tFrequency\n")
        f.write("-------\t\t---------\n")
        
        for i in range(min(6, len(sorted_counts))):
            number, count = sorted_counts[i]
            f.write(f"{number}\t\t{count}\n")
    
    # This message will still go to your Slurm log file.
    print(f"Report Generator: Finished. Final report written to {final_output_path}")


def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(description="MapReduce integer counting script.")
    parser.add_argument('phase', choices=['map', 'reduce', 'report'], help="The phase to execute.")
    parser.add_argument('--id', type=int, help="The ID for the mapper or reducer task.")

    args = parser.parse_args()
    
    setup_directories()

    if args.phase == 'map':
        if args.id is None:
            sys.exit("Error: Mapper phase requires a task --id.")
        run_mapper(args.id)
    elif args.phase == 'reduce':
        if args.id is None:
            sys.exit("Error: Reducer phase requires a task --id.")
        run_reducer(args.id)
    elif args.phase == 'report':
        run_report()


if __name__ == "__main__":
    main()