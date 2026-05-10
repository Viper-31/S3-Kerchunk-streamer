#!/bin/bash
#SBATCH --job-name=check_enc_zarr
#SBATCH --partition=work
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=00:10:00
#SBATCH --output=check_enc_zarr_%j.log
#SBATCH --error=check_enc_zarr_%j.err

module load python/3.11.6
cd $MYSCRATCH

# Activate the same virtual environment used for to_zarr.sh
source zarr_venv/bin/activate

echo "Starting Encoding Check at $(date)"

# Use python unbuffered flag and run the check script
python -u scripts/checks/check_enc_zarr.py

echo "Finished at $(date)"
