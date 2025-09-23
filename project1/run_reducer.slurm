#!/bin/bash
#SBATCH --job-name=proj1_reducer
#SBATCH --nodes=1
#SBATCH --time=00:15:00
#SBATCH --partition=extended-40core
#SBATCH --output=reducer_%j.out
#SBATCH --error=reducer_%j.err

module load anaconda/3
cd /gpfs/projects/AMS598/projects2025_submission/project1_submission/Mankala_RishiGuptha/
python3 count_integers.py reducer

