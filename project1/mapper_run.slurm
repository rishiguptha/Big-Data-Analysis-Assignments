#!/bin/bash
#SBATCH --job-name=map_integers     # Job name
#SBATCH --output=logs/map_%A_%a.out # Standard output log file (%A: job ID, %a: task ID)
#SBATCH --error=logs/map_%A_%a.err  # Standard error log file
#SBATCH --ntasks=1                  # Run a single task
#SBATCH --cpus-per-task=1           # Number of CPU cores per task
#SBATCH --mem=1G                    # Job memory request
#SBATCH --time=00:10:00             # Time limit hrs:min:sec
#SBATCH --array=0-3                 # Array of tasks (4 mappers, IDs 0-3)
#SBATCH --partition=debug-40core

# Load the required Python module on the cluster
module load python/3.11.2

echo "Starting mapper task $SLURM_ARRAY_TASK_ID"

# Run the python script in 'map' mode, passing the task ID
srun --export=ALL python mapreduce_counter.py map --id $SLURM_ARRAY_TASK_ID 
echo "Finished mapper task $SLURM_ARRAY_TASK_ID"