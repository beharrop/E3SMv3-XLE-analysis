#!/bin/bash

#SBATCH --job-name=get_era5_etccdi
#SBATCH --nodes=1
#SBATCH --output=get_era5_etccdi.o%j
#SBATCH --error=get_era5_etccdi.e%j
#SBATCH --time=1:10:00
#SBATCH --qos=regular
#SBATCH --account=e3sm
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

module load conda
conda activate myenv2

cd /global/cfs/cdirs/e3sm/beharrop/XLE/analysis_scripts/E3SMv3-XLE-analysis/

python etccdi.py
