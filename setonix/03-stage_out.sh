#!/bin/bash
#SBATCH --job-name=data_out_acacia
#SBATCH --partition=copy
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=8G
#SBATCH --output=data_out_%j.log
#SBATCH --error=data_out_%j.err

module load rclone/1.68.1

echo "Starting stage-out from $MYSCRATCH/vz_kerchunk to Acacia..."

rclone copy $MYSCRATCH/vz_kerchunk pawsey0411:weather/vz_kerchunk \
    --progress \
    --transfers 8 \
    --checkers 16

# Verify transfer success
if [ $? -eq 0 ]; then
    echo "Transfer successful. Verifying remote content..."
    rclone ls pawsey0411:weather/vz_kerchunk | head -n 20

else
    echo "Transfer failed. Scratch directories preserved for debugging."
    exit 1
fi

echo "Stage-out complete at $(date)."
