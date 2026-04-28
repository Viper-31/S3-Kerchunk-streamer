#!/bin/bash
#SBATCH --job-name=data_in_acacia
#SBATCH --partition=copy
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=8G
#SBATCH --output=data_in_%j.log
#SBATCH --error=data_in_%j.err

module load rclone/1.68.1

echo "Starting stage-in from Acacia to $MYSCRATCH/acacia_clean_data..."

mkdir -p $MYSCRATCH/acacia_clean_data

# Stage DPIRD Singleton
echo "Pulling DPIRD singleton..."
rclone copy pawsey0411:weather/FINAL_DPIRD/DPIRD_final_stations.nc \
    $MYSCRATCH/acacia_clean_data/ \
    --progress

# Stage ECMWF daily files
echo "Pulling ECMWF daily files..."
rclone copy pawsey0411:weather/ecmwf_op_clean/ \
    $MYSCRATCH/acacia_clean_data/ecmwf_op_clean/ \
    --include "[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9].nc" \
    --progress \
    --transfers 8 \
    --checkers 16

echo "Stage-in complete."
ls -R $MYSCRATCH/acacia_clean_data
