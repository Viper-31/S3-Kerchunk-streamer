#!/bin/bash
#SBATCH --job-name=chunk_n_compress
#SBATCH --partition=work
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --time=04:00:00
#SBATCH --output=chunk_compress_%j.log
#SBATCH --error=chunk_compress_%j.err

module load python/3.11.6
cd $MYSCRATCH
source zarr_venv/bin/activate

echo "Starting Chunking and Compression at $(date)"

python -u setonix/chunk_n_compress.py

echo "Finished at $(date)"
