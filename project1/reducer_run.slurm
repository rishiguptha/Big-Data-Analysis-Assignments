#!/bin/bash
#SBATCH --job-name=reduce_integers   # Job name
#SBATCH --output=logs/reduce_%A_%a.out # Standard output log
#SBATCH --error=logs/reduce_%A_%a.err  # Standard error log
#SBATCH --ntasks=1                   # Run a single task
#SBATCH --cpus-per-task=1            # Number of CPU cores per task
#SBATCH --mem=1G                     # Job memory request
#SBATCH --time=00:10:00              # Time limit hrs:min:sec
#SBATCH --array=0-3                  # Array of tasks (4 reducers, IDs 0-3)

# Load the required Python module
module load python/3.9.12

echo "Starting reducer task $SLURM_ARRAY_TASK_ID"

# Run the python script in 'reduce' mode, passing the task ID
srun python mapreduce_count.py reduce --id $SLURM_ARRAY_TASK_ID

echo "Finished reducer task $SLURM_ARRAY_TASK_ID"