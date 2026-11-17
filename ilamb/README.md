 ILAMB setup for the v3.XLE analysis

This directory contains the ILAMB configuration and launch scripts used to
compare the v3.XLE historical ensembles with selected CMIP6 models. Run the
smoke tests one analysis set at a time so failures can be attributed to `LR`,
`highECS`, `lowECS`, or the CMIP6 suite.

## Analysis configuration

- `ilamb.cfg` defines the observational confrontations and their weights.
- The `models_*.yaml` files are the model manifests for the benchmarking
  experiments (see the "Benchmarking experiments" section below).
- `regions.txt` defines the additional v3.XLE regions.
- `smoke_test.sh` validates and runs one selected analysis set.
- `ilamb_smoke.yaml` is retained for direct, LR-only ILAMB tests.
- `assets_ilamb_e3sm2/` is a reference copy of the upstream E3SM ILAMB assets.

### Benchmarking experiments

Eight production experiments compare the E3SM data against different CMIP6
references at two levels of aggregation: four at the ensemble-mean level and
four at the individual-member level. Each has its own model manifest, batch
script, and build directory, so their outputs and per-model `.pkl` caches never
collide.

Ensemble-mean experiments (E3SM ensemble means):

| Experiment | Manifest | Batch script | Build directory |
|---|---|---|---|
| E3SM vs CMIP6 individual models + CMIP6 ensemble | `models_e3sm_vs_cmip6_all.yaml` | `submit_e3sm_vs_cmip6_all.sh` | `.../www/sfeng/v3.XLE/_build_e3sm_vs_cmip6_all` |
| E3SM vs CMIP6 individual models only (no CMIP6 ensemble) | `models_e3sm_vs_cmip6ind.yaml` | `submit_e3sm_vs_cmip6ind.sh` | `.../www/sfeng/v3.XLE/_build_e3sm_vs_cmip6ind` |
| E3SM vs CMIP6 ensemble only | `models_e3sm_vs_cmip6ens.yaml` | `submit_e3sm_vs_cmip6ens.sh` | `.../www/sfeng/v3.XLE/_build_e3sm_vs_cmip6ens` |
| E3SM ensembles only | `models_e3sm_only.yaml` | `submit_e3sm_only.sh` | `.../www/sfeng/v3.XLE/_build_e3sm_only` |

These four differ only in which CMIP6 reference is included: `_all` has both the
five individual CMIP6 models and the CMIP6 multi-model mean (`CMIP6-MMM`);
`_cmip6ind` has the five individual CMIP6 models but no `CMIP6-MMM` (useful when
the multi-model mean would otherwise dominate or clutter the scorecard);
`_cmip6ens` has only `CMIP6-MMM`; and `e3sm_only` has no CMIP6 reference at all.

Member-level experiments (individual ensemble members):

| Experiment | Manifest | Batch script | Build directory |
|---|---|---|---|
| default (v3.LR) 25 members | `models_members_default.yaml` | `submit_members_default.sh` | `.../www/sfeng/v3.XLE/_build_members_default` |
| highECS 25 members | `models_members_highECS.yaml` | `submit_members_highECS.sh` | `.../www/sfeng/v3.XLE/_build_members_highECS` |
| lowECS 25 members | `models_members_lowECS.yaml` | `submit_members_lowECS.sh` | `.../www/sfeng/v3.XLE/_build_members_lowECS` |
| sample-5 members/family + 5 CMIP6 individual | `models_sample5_vs_cmip6.yaml` | `submit_sample5_vs_cmip6.sh` | `.../www/sfeng/v3.XLE/_build_sample5_vs_cmip6` |

Submit each with `sbatch submit_<name>.sh`. All eight reuse the same
`ilamb.cfg`, regions, `--study_limits 1985 2014`, `--rmse_score_basis cycle`,
and the tested MPI-9 Hydra launcher with the communicator-size guard.

The member manifests read individual historical members from
`../e3sm/output/{LR,highECS,lowECS}/`. Each family shares the same 25 suffixes
(`0051, 0091, 0101, …, 0321`). The `models_sample5_vs_cmip6.yaml` set uses a
fixed random sample of five suffixes (seed 42) reused across all three
families — `0051, 0111, 0161, 0281, 0311` — plus the five individual CMIP6
models.

Two confrontations are disabled in the shared `ilamb.cfg` for all eight sets
because no model provides a usable field: CO2 `NOAA.Emulated` (emulated `nbp`)
and Nitrogen Fixation `Davies-Barnard` (`fBNF`/`NFIX_TO_SMINN`). Both blocks are
commented out and can be re-enabled by uncommenting them.

Labeling convention (set via each manifest's `modelname` field, which is the
label ILAMB prints in figures and scorecards):

| YAML key | Label |
|---|---|
| `v3.LR` | `E3SMv3 default` |
| `v3.LR.highECS` | `E3SMv3 highECS` |
| `v3.LR.lowECS` | `E3SMv3 lowECS` |
| `CMIP6-MMM` | `CMIP6 ens` |

Member manifests extend this convention by appending the suffix, e.g.
`E3SMv3 default 0051`, `E3SMv3 highECS 0111`, `E3SMv3 lowECS 0281`.

The five individual CMIP6 models keep their native names.


The configured analysis regions are:

```text
global southamericaamazon eqas eqaf hilat temperate
```

The requested study limits are 1850–2014. ILAMB compares only the intersection
of those limits with each model and observation. In particular, the highECS and
lowECS ensemble means currently cover 1985–2014.

## Model sets

| Smoke-test set | Models | Input |
|---|---|---|
| `LR` | `v3.LR` | `../e3sm/output_ilamb/LR` |
| `highECS` | `v3.LR.highECS` | `../e3sm/output_ilamb/highECS` |
| `lowECS` | `v3.LR.lowECS` | `../e3sm/output_ilamb/lowECS` |
| `CMIP6` | `CMIP6-MMM`, `MIROC-ES2L`, `CanESM5`, `IPSL-CM6A-LR`, `NorESM2-LM`, and `UKESM1-0-LL` | Paths in the CMIP6 manifests |

`CMIP6-MMM` is the regridded multi-model mean under `../cmip6/output`. The
other five CMIP6 entries point to individually tracked historical model data.

## Software and observational data

The scripts use the `ilamb` conda environment:

```bash
source /global/cfs/cdirs/e3sm/feng809/code/miniconda3/bin/activate ilamb
```

The observational archive is exposed to ILAMB as `${ILAMB_ROOT}/DATA`:

```bash
export ILAMB_ROOT=/global/cfs/cdirs/e3sm/feng809/v3.XLE/ilamb
ln -sfn /global/cfs/cdirs/e3sm/feng809/data/ILAMB-DATA "${ILAMB_ROOT}/DATA"
```

`smoke_test.sh` performs both steps automatically inside the Slurm job.

## Running smoke tests

First run the inexpensive input-path preflight from a login node:

```bash
cd /global/cfs/cdirs/e3sm/feng809/v3.XLE/ilamb
./smoke_test.sh --preflight LR
./smoke_test.sh --preflight highECS
./smoke_test.sh --preflight lowECS
./smoke_test.sh --preflight CMIP6
```

Then submit one set at a time:

```bash
sbatch smoke_test.sh LR
sbatch smoke_test.sh highECS
sbatch smoke_test.sh lowECS
sbatch smoke_test.sh CMIP6
```

Do not submit all four commands simultaneously when the goal is sequential
diagnosis. Wait for one set to finish, inspect its report, and then submit the
next set.

Slurm writes console output to `smoke_test.<jobid>.log` in this directory.
Each job writes its ILAMB products and detailed logs to a separate location:

```text
/global/cfs/cdirs/e3sm/www/sfeng/v3.XLE/_smoke_test/<set>/<jobid>/
```

The smoke script exits nonzero if `ilamb-run` fails or if an ILAMB log contains
a failed model/confrontation pair. Its final error summary uses the form:

```text
<model>: <confrontation>
```

This makes a CMIP6-suite failure attributable to a specific CMIP6 model even
though the six CMIP6 entries run as one set.

## MPI launcher on Perlmutter

The conda environment contains Intel MPI. In this environment, launching
`mpi4py` with `srun` produced unrelated size-one MPI communicators, causing all
processes to behave as rank 0 and collide while creating ILAMB output files.
The smoke script therefore uses:

```bash
mpiexec -bootstrap slurm -n 9 ilamb-run ...
```

It first asserts that MPI created a size-nine communicator. Do not remove this
guard or replace the tested launcher with `srun` without revalidating the MPI
rank and size behavior.

## Production analysis

The production analyses are the eight benchmarking experiments documented in the
"Benchmarking experiments" section above. Each writes to its own build directory
under `/global/cfs/cdirs/e3sm/www/sfeng/v3.XLE/_build_*` and is submitted with
`sbatch submit_<name>.sh`. Run all smoke-test sets successfully before
submitting. Every submit script uses the same Hydra Slurm bootstrap and
communicator-size guard as the tested smoke script.

## Reruns and caching

ILAMB (2.7.2 here) is incremental. Inside each `--build_dir` it caches results
at two levels: a per-model file at the top of the build directory (for example
`_build_members_default/v3.LR.historical_0051.pkl`) and the per
model-confrontation `.nc` files and figures under the confrontation subtrees
(for example `EcosystemandCarbonCycle/Biomass/GEOCARBON/`). Before computing any
model-confrontation pair, ILAMB checks whether its output already exists in the
build directory and, if so, reuses it instead of recomputing. This is why a
resubmitted job resumes where it left off rather than starting over.

Whether you need to clear old results before rerunning depends on why you are
rerunning:

| Situation | Clear old results? | Why |
|---|---|---|
| Job timed out or crashed; same inputs and config | No | Resume is desired: reuse the cache and finish the remaining work. |
| Model input data changed (e.g. reprocessed a variable) | Yes | ILAMB will not detect newer inputs and would silently reuse stale cache. |
| `ilamb.cfg` changed (weights, scoring, confrontation options) | Yes | New confrontations are computed, but changed scoring on already-cached pairs is not recomputed unless cleared. |
| Added a brand-new model to the manifest | No | Only the new model is computed; existing models are reused. |
| Want a guaranteed-clean, reproducible run | Yes | Removes any chance of stale cache contaminating results. |

To force a clean recompute, use either approach:

- Add `--clean` to the `ilamb-run` line in the submit script. Per
  `ilamb-run --help`, this removes the cached analysis files and recomputes
  everything in that build directory. This is the recommended approach.
- Or delete the build directory (or just its `*.pkl` files and confrontation
  subdirectories) before resubmitting. Because each experiment writes to its own
  `_build_*` directory, you can wipe only the one you are rerunning without
  affecting the others.

Note: if you reprocess or otherwise change the input data, always do a clean
rerun so ILAMB does not reuse cache generated from the old data.

### Partial cache from an interrupted pair

There is one failure mode the table above does not cover: a model-confrontation
pair whose computation was interrupted (job timeout or a transient MPI/worker
hiccup) partway through writing its output. ILAMB writes the pair's `.nc` file
and some figures before it finishes all of them, so an interrupted pair can be
left on disk in an incomplete state. On the next resubmit, ILAMB sees that
output already exists, logs `UsingCachedData`, and "completes" the pair in a few
seconds without regenerating the missing figures/scores.

Symptoms:

- A single model-confrontation renders with far fewer files than the same pair
  in another build (for example only the `*_timeint.png` and the `.nc`, but no
  `*_bias*`, `*_rmse*`, `*_cycle*`, `*_phase*`, or `*score*` plots).
- The job log shows `UsingCachedData` and a suspiciously short completion time
  (seconds) for that pair, while the equivalent pair elsewhere took minutes.

Fix — clear only the affected member/confrontation, then resubmit so it
recomputes while every healthy pair keeps its good cache:

1. Delete that model's top-level `.pkl` (its name is the manifest YAML key, for
   example `v3.LR.highECS.historical_0161.pkl`).
2. Delete the stale files for that model under the affected confrontation
   subtree. Output files are named with the model's `modelname` label, for
   example `.../HydrologyCycle/EvaporativeFraction/FLUXCOM/*E3SMv3 highECS 0161*`.
3. Resubmit `submit_<name>.sh`. ILAMB recomputes the cleared pair and reuses the
   cache for everything else.

This is more surgical than `--clean`, which would recompute every model in the
build directory. Reserve `--clean` for changed inputs, changed `ilamb.cfg`, or a
guaranteed-clean reproducible run.

## Known data limitations

The current E3SM ensemble-mean directories contain land output only. The
ensemble builder reads `post/lnd/180x360_aave/cmip_ts/monthly`; it does not yet
include the separate atmosphere collection. Consequently, missing atmosphere
variables can produce `VarNotInModel` rows even when source atmosphere files
exist.

- highECS and lowECS have complete 1985–2014 source atmosphere extractions for
  the standard fields inspected in the audit.
- LR has incomplete atmosphere extraction, including missing `tasmax` and
  `tasmin` and incomplete radiation and latent-heat fields.
- Standard fields absent from the inspected E3SM postprocessed output include
  `hurs`, snow water equivalent, burned area, nitrogen fixation, and a directly
  usable total-soil-carbon field.
- The current CMIP6 multi-model mean contains 11 variables: `cVeg`,
  `evspsblveg`, `gpp`, `lai`, `mrro`, `mrsos`, `nbp`, `prveg`, `ra`, `rh`, and
  `tsl`. Diagnostics requiring other fields will be unavailable for
  `CMIP6-MMM` until those ensemble means are created.
- LR includes aggregate files that overlap shorter time chunks. The curated
  `output_ilamb/LR` view excludes the overlapping aggregate files.

Complete extraction findings and recommended data-preparation work are in
[`../e3sm/E3SM_EXTRACTION_AUDIT.md`](../e3sm/E3SM_EXTRACTION_AUDIT.md).

## Interpreting common failures

- `VarNotInModel` means ILAMB could not find the required variable, an accepted
  alternate, or all inputs for a configured derived variable.
- A Python exception such as `TypeError` means the variable was found but the
  confrontation failed while reading, converting, comparing, or plotting it.
- `PermissionError` usually indicates concurrent jobs or MPI ranks tried to
  write the same output. Per-set, per-job build directories prevent this in the
  current smoke workflow.

Use the detailed `ILAMB*.log` files in the job's build directory to distinguish
missing input from processing errors before changing the model data.
