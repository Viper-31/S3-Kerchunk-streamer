#!/bin/bash
#SBATCH --job-name=to_zarr
#SBATCH --partition=work
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=200G
#SBATCH --time=02:00:00
#SBATCH --output=to_zarr_%j.log
#SBATCH --error=to_zarr_%j.err

module load python/3.11.6
cd $MYSCRATCH
source zarr_venv/bin/activate

echo "Starting to Zarr conversion at $(date)"

python -u scripts/02.2-to_zarr.py

echo "Finished at $(date)"