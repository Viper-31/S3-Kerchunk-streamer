#!/bin/bash
#SBATCH --job-name=to_zarr_dry_run
#SBATCH --partition=work
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=50G
#SBATCH --time=01:00:00
#SBATCH --output=to_zarr_dry_run_%j.log
#SBATCH --error=to_zarr_dry_run_%j.err

module load python/3.11.6
cd $MYSCRATCH
source zarr_venv/bin/activate

echo "Starting to Zarr conversion at $(date)"

python -u scripts/02.2-to_zarr.py --dry-run

echo "Finished at $(date)"