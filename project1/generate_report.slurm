#!/bin/bash
#SBATCH --job-name=mapreduce-report
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=2G
#SBATCH --partition=short

set -euo pipefail

PY=python3
WORK_DIR="/gpfs/projects/AMS598/class2025/rmankala/"
SCRIPT="${WORK_DIR}/map_reduce_pipeline.py"

srun -N1 -n1 ${PY} "${SCRIPT}" --phase report --work-dir "${WORK_DIR}"
echo "Final report available at ${WORK_DIR}/output.txt"
