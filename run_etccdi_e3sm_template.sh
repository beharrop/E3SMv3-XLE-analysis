#!/bin/bash

#SBATCH --job-name=get_etccdi_{casename}
#SBATCH --nodes=1
#SBATCH --output=get_etccdi_{casename}.o%j
#SBATCH --error=get_etccdi_{casename}.e%j
#SBATCH --time=0:18:00
#SBATCH --qos=regular
#SBATCH --account=e3sm
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

module load conda
conda activate myenv2

cd /global/cfs/cdirs/e3sm/beharrop/XLE/analysis_scripts/E3SMv3-XLE-analysis/

python etccdi.py --source e3sm --case {casename}
