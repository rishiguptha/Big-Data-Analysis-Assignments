#!/bin/bash
#SBATCH --job-name=mapreduce_pipeline # A single name for both phases
#SBATCH --output=logs/pipeline_%A_%a.out # Job ID and Task ID in logs
#SBATCH --error=logs/pipeline_%A_%a.err
#SBATCH --partition=short-28core     # The partition we found that works
#SBATCH --ntasks=1                   # Each task runs on one core
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G                     # Request 1 gigabyte of memory
#SBATCH --time=00:15:00              # A 15-minute time limit
#SBATCH --array=0-7                  # A job array of 8 tasks (4 mappers, 4 reducers)

# --- Environment Setup ---
# Always load the specific Python module you've tested and confirmed is available.
echo "Loading Python environment..."
module load python/3.11.2

TASK_ID=$SLURM_ARRAY_TASK_ID
NUM_MAPPERS=4

# --- Task Orchestration ---
# This logic determines whether a task should act as a mapper or a reducer.
if [ $TASK_ID -lt $NUM_MAPPERS ]; then
    # Tasks 0-3 will be Mappers
    echo "Starting Mapper Task ID: $TASK_ID"
    srun --export=ALL python map_reduce_pipeline.py map --id $TASK_ID
else
    # Tasks 4-7 will be Reducers
    # We subtract NUM_MAPPERS to get reducer IDs from 0 to 3 (e.g., 4-4=0, 5-4=1)
    REDUCER_ID=$((TASK_ID - NUM_MAPPERS))
    echo "Starting Reducer Task ID: $REDUCER_ID"
    srun --export=ALL python map_reduce_pipeline.py reduce --id $REDUCER_ID
fi

echo "Task $TASK_ID has finished."
