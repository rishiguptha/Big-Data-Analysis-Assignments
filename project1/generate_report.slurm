#!/bin/bash
#SBATCH --job-name=generate_report
#SBATCH --output=logs/report_%j.out # %j is the Job ID
#SBATCH --error=logs/report_%j.err
#SBATCH --partition=short-28core
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M                # Less memory is needed for this step
#SBATCH --time=00:05:00           # A 5-minute time limit is plenty

# --- Environment Setup ---
echo "Loading Python environment..."
module load python/3.11.2

# --- Run the Report Generation ---
echo "Starting the final report generation..."
srun --export=ALL python map_reduce_pipeline.py report

echo "Report generation complete. Check for output.txt."