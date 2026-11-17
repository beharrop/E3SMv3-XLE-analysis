#!/bin/bash

# Test one analysis set at a time so errors can be attributed to that set:
#   sbatch smoke_test.sh LR
#   sbatch smoke_test.sh highECS
#   sbatch smoke_test.sh lowECS
#   sbatch smoke_test.sh CMIP6
# The same commands can be run from an equivalent interactive allocation.

#SBATCH --nodes=1
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --account=e3sm
#SBATCH --time=02:00:00
#SBATCH --ntasks=9
#SBATCH --cpus-per-task=14
#SBATCH --job-name=ilamb-smoke
#SBATCH --output=smoke_test.%j.log

set -eo pipefail
shopt -s globstar nullglob

export ILAMB_ROOT=/global/cfs/cdirs/e3sm/feng809/v3.XLE/ilamb
readonly MODEL_SETUP="${ILAMB_ROOT}/models_combined.yaml"
readonly BUILD_ROOT=/global/cfs/cdirs/e3sm/www/sfeng/v3.XLE/_smoke_test
readonly RUN_TASKS=9
readonly -a ANALYSIS_REGIONS=(global southamericaamazon eqas eqaf hilat temperate)

preflight_only=false
case "${1:-}" in
  --preflight)
    preflight_only=true
    shift
    ;;
esac

if (( $# != 1 )); then
  echo "Usage: $0 [--preflight] {LR|highECS|lowECS|CMIP6}" >&2
  exit 2
fi

readonly SUITE="$1"
case "${SUITE}" in
  LR)
    readonly -a SUITE_MODELS=(v3.LR)
    ;;
  highECS)
    readonly -a SUITE_MODELS=(v3.LR.highECS)
    ;;
  lowECS)
    readonly -a SUITE_MODELS=(v3.LR.lowECS)
    ;;
  CMIP6)
    readonly -a SUITE_MODELS=(CMIP6-MMM MIROC-ES2L CanESM5 IPSL-CM6A-LR NorESM2-LM UKESM1-0-LL)
    ;;
  *)
    echo "ERROR: unknown analysis set: ${SUITE}" >&2
    echo "Usage: $0 [--preflight] {LR|highECS|lowECS|CMIP6}" >&2
    exit 2
    ;;
esac

declare -A selected_models=()
for model_name in "${SUITE_MODELS[@]}"; do
  selected_models["${model_name}"]=1
done

cd "${ILAMB_ROOT}"
echo "Smoke-test analysis set: ${SUITE}"
echo "Models: ${SUITE_MODELS[*]}"

# Fail before the expensive ILAMB run if a model in the analysis manifest has
# no readable NetCDF input. Include symlinks because the E3SM manifests use
# links into e3sm/output.
model_count=0
while IFS='|' read -r model_name model_path; do
  [[ -n "${model_name}" && -n "${model_path}" ]] || continue
  [[ -n "${selected_models[${model_name}]:-}" ]] || continue
  model_count=$((model_count + 1))

  if [[ ! -d "${model_path}" ]]; then
    echo "ERROR: ${model_name}: model path does not exist: ${model_path}" >&2
    exit 1
  fi

  readable_input=false
  nc_files=("${model_path}"/**/*.nc)
  for nc_file in "${nc_files[@]}"; do
    if [[ -r "${nc_file}" ]]; then
      readable_input=true
      break
    fi
  done

  if [[ "${readable_input}" != true ]]; then
    echo "ERROR: ${model_name}: no readable NetCDF inputs under ${model_path}" >&2
    exit 1
  fi

  echo "Input ready: ${model_name} (${model_path})"
done < <(
  awk '
    /^[^[:space:]#][^:]*:[[:space:]]*$/ {
      model = $0
      sub(/:[[:space:]]*$/, "", model)
    }
    /^[[:space:]]+path:[[:space:]]*/ {
      path = $0
      sub(/^[[:space:]]+path:[[:space:]]*/, "", path)
      gsub(/^"|"$/, "", path)
      print model "|" path
    }
  ' "${MODEL_SETUP}"
)

if (( model_count != ${#SUITE_MODELS[@]} )); then
  echo "ERROR: found ${model_count} of ${#SUITE_MODELS[@]} requested ${SUITE} models in ${MODEL_SETUP}" >&2
  exit 1
fi

if [[ "${preflight_only}" == true ]]; then
  echo "Preflight passed for ${SUITE} (${model_count} model(s))."
  exit 0
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
  echo "ERROR: submit $0 with sbatch or run it inside a Perlmutter CPU allocation" >&2
  exit 2
fi

source /global/cfs/cdirs/e3sm/feng809/code/miniconda3/bin/activate ilamb
set -u
readonly BUILD_DIR="${BUILD_ROOT}/${SUITE}/${SLURM_JOB_ID}"
ln -sfn /global/cfs/cdirs/e3sm/feng809/data/ILAMB-DATA "${ILAMB_ROOT}/DATA"
mkdir -p "${BUILD_DIR}"

ilamb_args=(
  --config ilamb.cfg
  --model_setup "${MODEL_SETUP}"
  --models "${SUITE_MODELS[@]}"
  --define_regions "${ILAMB_ROOT}/DATA/regions/GlobalLand.nc"
                   "${ILAMB_ROOT}/DATA/regions/LandRegions.nc"
                   "${ILAMB_ROOT}/regions.txt"
  --regions "${ANALYSIS_REGIONS[@]}"
  --study_limits 1985 2014
  --rmse_score_basis cycle
  --title "v3.XLE ILAMB smoke test: ${SUITE}"
  --build_dir "${BUILD_DIR}"
)

# This conda environment contains Intel MPI. Hydra's Slurm bootstrap creates
# one shared communicator; srun currently starts the mpi4py processes as
# unrelated MPI singletons on Perlmutter. Verify this before ILAMB touches the
# shared build directory.
export ILAMB_SMOKE_EXPECTED_MPI_SIZE="${RUN_TASKS}"
if ! mpiexec -bootstrap slurm -n "${RUN_TASKS}" python -c \
  'from mpi4py import MPI; import os; assert MPI.COMM_WORLD.size == int(os.environ["ILAMB_SMOKE_EXPECTED_MPI_SIZE"])'; then
  echo "ERROR: MPI launcher did not create a ${RUN_TASKS}-rank communicator" >&2
  exit 1
fi

if ! mpiexec -bootstrap slurm -n "${RUN_TASKS}" ilamb-run \
  "${ilamb_args[@]}" \
  --clean; then
  echo "ERROR: ilamb-run exited unsuccessfully" >&2
  exit 1
fi

# ILAMB 2.7.2 returns zero even when individual model/confrontation pairs fail.
# Treat those logged failures as a failed smoke test so incomplete inputs cannot
# pass silently. MPI ranks write separate ILAMB*.log files.
ilamb_logs=("${BUILD_DIR}"/ILAMB*.log)
if (( ${#ilamb_logs[@]} == 0 )); then
  echo "ERROR: no ILAMB logs were created under ${BUILD_DIR}" >&2
  exit 1
fi

failure_found=false
for ilamb_log in "${ilamb_logs[@]}"; do
  if grep -Fq '][WorkConfront]' "${ilamb_log}"; then
    failure_found=true
    echo "Failed model/confrontation pairs in ${ilamb_log}:" >&2
    awk -F '[][]' '$6 == "WorkConfront" {print "  " $10 ": " $8}' "${ilamb_log}" \
      | sort -u >&2
  fi
done

if [[ "${failure_found}" == true ]]; then
  echo "ERROR: one or more ILAMB model/confrontation pairs failed." >&2
  echo "Full logs: ${BUILD_DIR}/ILAMB*.log" >&2
  exit 1
fi

echo "Smoke test passed for ${SUITE} (${model_count} model(s)) in all analysis regions. Results: ${BUILD_DIR}"
