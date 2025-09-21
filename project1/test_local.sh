#!/bin/bash

# A script to simulate the MapReduce workflow on a local machine.

echo "--- Starting Local MapReduce Test ---"

# Clean up previous runs to ensure a fresh start
echo "Step 0: Cleaning up old intermediate and output files..."
rm -rf intermediate/
rm -rf output/
echo "Cleanup complete."
echo

# --- MAP PHASE ---
# Run the 4 mapper processes in parallel.
echo "Step 1: Starting Map Phase..."
for i in {0..3}
do
  python mapreduce_count.py map --id $i &
done

# Wait for all background map processes to finish before proceeding.
wait
echo "Map Phase complete."
echo

# --- REDUCE PHASE ---
# Run the 4 reducer processes in parallel.
echo "Step 2: Starting Reduce Phase..."
for i in {0..3}
do
  python mapreduce_count.py reduce --id $i &
done

# Wait for all background reduce processes to finish.
wait
echo "Reduce Phase complete."
echo

# --- REPORT PHASE ---
# Run the final report generator.
echo "Step 3: Starting Report Phase..."
python mapreduce_count.py report
echo

echo "--- Local MapReduce Test Finished ---"