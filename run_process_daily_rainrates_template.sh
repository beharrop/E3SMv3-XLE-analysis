#!/bin/bash

#SBATCH --job-name=process_daily_rainrates_{ensemble}
#SBATCH --nodes=1
#SBATCH --output=process_daily_rainrates_{ensemble}.o%j
#SBATCH --error=process_daily_rainrates_{ensemble}.e%j
#SBATCH --time=4:30:00
#SBATCH --qos=regular
#SBATCH --account=e3sm
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

cd /global/cfs/cdirs/e3sm/beharrop/XLE/analysis_scripts/E3SMv3-XLE-analysis/

python process_daily_rainrates.py \
       --ensemble_members {ensemble} \
       --year_start 1985 \
       --year_final 2014
