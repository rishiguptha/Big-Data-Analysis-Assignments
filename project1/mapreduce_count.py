# mapreduce_count.py
import os
import sys
import glob
from collections import defaultdict
import argparse

# --- Configuration ---
# This script is now configured for local execution.
# It determines paths relative to its own location.

# Get the directory where this script is located.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set the base project directory to the script's directory.
BASE_PROJECT_DIR = SCRIPT_DIR

# IMPORTANT: Assumes the data is in a subdirectory named 'project1_data'
# relative to where the script is.
INPUT_DATA_DIR = os.path.join(BASE_PROJECT_DIR, "project1_data")

# Directories for temporary and final outputs.
INTERMEDIATE_DIR = os.path.join(BASE_PROJECT_DIR, "intermediate")
OUTPUT_DIR = os.path.join(BASE_PROJECT_DIR, "output")

# Number of parallel processes for mappers and reducers.
NUM_MAPPERS = 4
NUM_REDUCERS = 4


def setup_directories():
    """
    Creates the necessary intermediate and output directories if they don't exist.
    This prevents errors when scripts try to write files.
    """
    # Check if the input directory exists before proceeding.
    if not os.path.isdir(INPUT_DATA_DIR):
        print(f"Error: Input data directory not found at '{INPUT_DATA_DIR}'")
        print("Please make sure your data is in the correct sub-directory.")
        sys.exit(1)
        
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


# (The rest of the file remains exactly the same)
# ... run_mapper(mapper_id) ...
# ... run_reducer(reducer_id) ...
# ... run_report() ...
# ... main() ...

def run_mapper(mapper_id):
    """
    Mapper function.
    Reads a subset of input files, counts integer frequencies, and writes
    the local counts to an intermediate file on disk.
    
    Args:
        mapper_id (int): The ID of this mapper process (e.g., 0, 1, 2, or 3).
    """
    print(f"Mapper {mapper_id}: Starting...")
    
    # Get a list of all input data files.
    all_files = sorted(glob.glob(os.path.join(INPUT_DATA_DIR, "*.txt")))
    
    # Determine which files this mapper is responsible for.
    # This simple division splits the work evenly.
    files_per_mapper = len(all_files) // NUM_MAPPERS
    start_index = mapper_id * files_per_mapper
    end_index = start_index + files_per_mapper
    
    # Ensure the last mapper gets any remaining files if division isn't perfect.
    if mapper_id == NUM_MAPPERS - 1:
        end_index = len(all_files)

    assigned_files = all_files[start_index:end_index]
    print(f"Mapper {mapper_id}: Assigned {len(assigned_files)} files.")

    # Use a dictionary to store local counts for this mapper's files.
    local_counts = defaultdict(int)

    # Process each assigned file.
    for filepath in assigned_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    # Each line is an integer. Clean it up and count it.
                    num = int(line.strip())
                    local_counts[num] += 1
                except (ValueError, TypeError):
                    # Skip lines that aren't valid integers.
                    continue
    
    # Write the intermediate results to a dedicated file for this mapper.
    # The format is "integer<space>count\n" for easy parsing by reducers.
    intermediate_filepath = os.path.join(INTERMEDIATE_DIR, f"map_{mapper_id}.txt")
    with open(intermediate_filepath, 'w') as f:
        for number, count in local_counts.items():
            f.write(f"{number} {count}\n")
            
    print(f"Mapper {mapper_id}: Finished. Intermediate results saved to {intermediate_filepath}")


def run_reducer(reducer_id):
    """
    Reducer function.
    Reads all intermediate files from mappers, but only processes the integers
    that are assigned to it based on a hash function (modulo). It sums up the
    counts for its assigned integers and writes the final result to an output file.

    Args:
        reducer_id (int): The ID of this reducer process (e.g., 0, 1, 2, or 3).
    """
    print(f"Reducer {reducer_id}: Starting...")
    
    # This dictionary will hold the final counts for the integers this reducer is responsible for.
    final_counts = defaultdict(int)

    # Reducers need to read from ALL mapper outputs.
    intermediate_files = glob.glob(os.path.join(INTERMEDIATE_DIR, "map_*.txt"))
    
    for filepath in intermediate_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split()
                    if len(parts) == 2:
                        number, count = int(parts[0]), int(parts[1])
                        
                        # This is the core of the reduce phase distribution.
                        # We use the modulo operator to assign each number to a specific reducer.
                        if number % NUM_REDUCERS == reducer_id:
                            final_counts[number] += count
                except (ValueError, TypeError):
                    continue

    # Write the final aggregated counts to a dedicated file for this reducer.
    output_filepath = os.path.join(OUTPUT_DIR, f"reduce_{reducer_id}.txt")
    with open(output_filepath, 'w') as f:
        for number, count in final_counts.items():
            f.write(f"{number} {count}\n")
            
    print(f"Reducer {reducer_id}: Finished. Final counts saved to {output_filepath}")


def run_report():
    """
    Final reporting step.
    Collects the results from all reducer output files, aggregates them,
    and prints the top 6 integers with the highest frequencies.
    """
    print("Report Generator: Starting...")
    
    total_counts = {}
    
    # Read all the final output files from the reducers.
    output_files = glob.glob(os.path.join(OUTPUT_DIR, "reduce_*.txt"))

    if not output_files:
        print("Error: No reducer output files found. Did the reducer step run correctly?")
        return

    for filepath in output_files:
        with open(filepath, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split()
                    if len(parts) == 2:
                        number, count = int(parts[0]), int(parts[1])
                        total_counts[number] = count
                except (ValueError, TypeError):
                    continue
                    
    # Sort the final counts by frequency in descending order.
    # The key for sorting is the second element (the count) of each item.
    sorted_counts = sorted(total_counts.items(), key=lambda item: item[1], reverse=True)
    
    # Print the final report header.
    print("\n--- Final Report: Top 6 Integers by Frequency ---")
    print("Integer\t\tFrequency")
    print("-------\t\t---------")
    
    # Print the top 6 results.
    for i in range(min(6, len(sorted_counts))):
        number, count = sorted_counts[i]
        print(f"{number}\t\t{count}")
    
    print("\nReport Generator: Finished.")


def main():
    """
    Main entry point for the script.
    Uses argparse to determine which phase (map, reduce, report) to run.
    """
    # Set up the command-line argument parser.
    parser = argparse.ArgumentParser(description="MapReduce integer counting script.")
    parser.add_argument('phase', choices=['map', 'reduce', 'report'], help="The phase to execute.")
    parser.add_argument('--id', type=int, help="The ID for the mapper or reducer task.")

    args = parser.parse_args()
    
    # Always ensure directories are ready.
    setup_directories()

    if args.phase == 'map':
        if args.id is None:
            print("Error: Mapper phase requires a task --id.")
            sys.exit(1)
        run_mapper(args.id)
    elif args.phase == 'reduce':
        if args.id is None:
            print("Error: Reducer phase requires a task --id.")
            sys.exit(1)
        run_reducer(args.id)
    elif args.phase == 'report':
        run_report()


if __name__ == "__main__":
    main()