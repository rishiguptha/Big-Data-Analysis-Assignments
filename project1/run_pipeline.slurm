#!/bin/bash
#SBATCH --job-name=mapreduce-pipeline
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=8G
#SBATCH --partition=short

set -euo pipefail

PY=python3
DATA_DIR="/gpfs/projects/AMS598/projects2025_data/project1_data/"
WORK_DIR="/gpfs/projects/AMS598/class2025/rmankala/"
SCRIPT="${WORK_DIR}/map_reduce_pipeline.py"

mkdir -p "${WORK_DIR}/intermediate"

for i in 0 1 2 3; do
  srun -N1 -n1 ${PY} "${SCRIPT}" --phase map --data-dir "${DATA_DIR}" --work-dir "${WORK_DIR}" --task-index ${i} --num-tasks 4 &
done
wait

for i in 0 1 2 3; do
  srun -N1 -n1 ${PY} "${SCRIPT}" --phase reduce --work-dir "${WORK_DIR}" --task-index ${i} --num-tasks 4 &
done
wait
