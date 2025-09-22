#!/bin/bash
#SBATCH --job-name=report_integers    # Job name
#SBATCH --output=logs/report_%A.out   # Standard output log
#SBATCH --error=logs/report_%A.err    # Standard error log
#SBATCH --ntasks=1                    # Run a single task
#SBATCH --cpus-per-task=1             # Number of CPU cores per task
#SBATCH --mem=512M                    # This step needs less memory
#SBATCH --time=00:02:00               # This step is very fast

# Load the required Python module
module load python/3.9.12

echo "Starting final report generation..."

# Run the python script in 'report' mode
srun python mapreduce_counter.py report

echo "Report generation complete. Check the output log."