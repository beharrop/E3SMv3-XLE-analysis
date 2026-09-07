#!/bin/bash

# Create CDAT/CDMS xml description files for input data
source /global/common/software/e3sm/anaconda_envs/load_e3sm_unified_1.11.0_pm-cpu.sh

Basepath=/global/cfs/cdirs/e3smdata/simulations/v3.XLE
ensembles=(0051 0091 0101 0111 0121 
           0131 0141 0151 0161 0171
           0181 0191 0201 0211 0221 
           0231 0241 0251 0261 0271
           0281 0291 0301 0311 0321)

mkdir -p E3SMv3 E3SMv3.lowECS E3SMv3.highECS

# --- Default ---
for ens in "${ensembles[@]}"; do
    CASENAME=v3.LR.historical_${ens}
    OUTDIR=E3SMv3
    DATADIR=$Basepath/$CASENAME/post/atm/180x360_aave/ts/monthly/5yr
    if [ -f "${OUTDIR}/${CASENAME}.xml" ]; then
        echo "Skipping ${CASENAME}: xml already exists"
        continue
    fi
    if ! compgen -G "${DATADIR}/TS_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/TREFHT_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/OCNFRAC_??????_??????.nc" > /dev/null 2>&1; then
        echo "Skipping ${CASENAME}: missing TS/TREFHT/OCNFRAC data"
        continue
    fi
    cdscan -x ${OUTDIR}/${CASENAME}.xml ${DATADIR}/{TS,TREFHT,OCNFRAC}_??????_??????.nc
done

# --- lowECS ---
for ens in "${ensembles[@]}"; do
    CASENAME=v3.LR.lowECS.historical_${ens}
    OUTDIR=E3SMv3.lowECS
    DATADIR=$Basepath/$CASENAME/post/atm/180x360_aave/ts/monthly/5yr
    if [ -f "${OUTDIR}/${CASENAME}.xml" ]; then
        echo "Skipping ${CASENAME}: xml already exists"
        continue
    fi
    if ! compgen -G "${DATADIR}/TS_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/TREFHT_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/OCNFRAC_??????_??????.nc" > /dev/null 2>&1; then
        echo "Skipping ${CASENAME}: missing TS/TREFHT/OCNFRAC data"
        continue
    fi
    cdscan -x ${OUTDIR}/${CASENAME}.xml ${DATADIR}/{TS,TREFHT,OCNFRAC}_??????_??????.nc
done

# --- highECS ---
for ens in "${ensembles[@]}"; do
    CASENAME=v3.LR.highECS.historical_${ens}
    OUTDIR=E3SMv3.highECS
    DATADIR=$Basepath/$CASENAME/post/atm/180x360_aave/ts/monthly/5yr
    if [ -f "${OUTDIR}/${CASENAME}.xml" ]; then
        echo "Skipping ${CASENAME}: xml already exists"
        continue
    fi
    if ! compgen -G "${DATADIR}/TS_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/TREFHT_??????_??????.nc" > /dev/null 2>&1 ||
       ! compgen -G "${DATADIR}/OCNFRAC_??????_??????.nc" > /dev/null 2>&1; then
        echo "Skipping ${CASENAME}: missing TS/TREFHT/OCNFRAC data"
        continue
    fi
    cdscan -x ${OUTDIR}/${CASENAME}.xml ${DATADIR}/{TS,TREFHT,OCNFRAC}_??????_??????.nc
done
