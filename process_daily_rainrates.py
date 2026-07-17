"""process_daily_rainrates.py

Loop through the v3.LR.historical, 
                 v3.LR.lowECS.historical, and
                 v3.LR.highECS.historical ensembles.

For each member that does not yet have:
    /global/cfs/cdirs/e3smdata/simulations/v3.XLE/{casename}/
        post/atm/native/rainhist/daily/1yr/RRA_{year}.nc
call compute_rainrate_amounts for that member and year

For each member that does not yet have:
    /global/cfs/cdirs/e3smdata/simulations/v3.XLE/{casename}/
        post/atm/native/rainhist/daily/1yr/RRC_{year}.nc
call compute_rainrate_counts for that member and year

Run:  python process_daily_rainrates.py
"""

import argparse
import os
from pathlib import Path
from rainrate_amount_calculator import compute_rainrate_amounts
from rainrate_count_calculator import compute_rainrate_counts

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR   = Path(__file__).parent
BASE_SIM_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")
IN_RELPATH   = "post/atm/native/ts/daily/5yr/"
OUT_RELPATH  = "post/atm/native/rainhist/daily/1yr/"

# ---------------------------------------------------------------------------
# Ensemble member registry  (mirrors manage_data.py _ENSEMBLE_MEMBERS)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Defaults (used when no CLI arguments are provided)
# ---------------------------------------------------------------------------

DEFAULT_ENSEMBLE_MEMBERS = ["0051"]
DEFAULT_YEAR_START = 1985
DEFAULT_YEAR_FINAL = 2014

def get_in_file(year, year_start, year_final):
    if not (year_start <= year <= year_final):
        raise ValueError(f"Year {year} is out of the supported range.")
    chunk_start = (year // 5) * 5
    return f"PRECT_{chunk_start}01_{chunk_start + 4}12.nc"
    

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute rainrate amount and count histograms for XLE ensemble members."
    )
    parser.add_argument(
        "--ensemble_members",
        default=",".join(DEFAULT_ENSEMBLE_MEMBERS),
        help=(
            "Comma-separated list of ensemble member IDs to process "
            f"(default: {','.join(DEFAULT_ENSEMBLE_MEMBERS)})"
        ),
    )
    parser.add_argument(
        "--year_start",
        type=int,
        default=DEFAULT_YEAR_START,
        help=f"First year to process (default: {DEFAULT_YEAR_START})",
    )
    parser.add_argument(
        "--year_final",
        type=int,
        default=DEFAULT_YEAR_FINAL,
        help=f"Last year to process (default: {DEFAULT_YEAR_FINAL})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ensemble_members = [m.strip() for m in args.ensemble_members.split(",")]
    year_start = args.year_start
    year_final = args.year_final

    for ensemble in ["v3.LR.historical", "v3.LR.lowECS.historical", "v3.LR.highECS.historical"]:
        for member_id in ensemble_members:
            casename = f"{ensemble}_{member_id}"
            out_path = os.path.join(BASE_SIM_DIR, casename, OUT_RELPATH)
            os.makedirs(out_path, exist_ok=True)
            for year in range(year_start, year_final + 1):
                print(f"Processing year {year} for {casename}, member {member_id}")
                for calc, out_tag in zip([compute_rainrate_amounts, compute_rainrate_counts],
                                         ['RRA', 'RRC']):
                    out_file = os.path.join(out_path, f'{out_tag}_{year}.nc')
                    if os.path.exists(out_file):
                        print(f"[SKIP]    {casename}, {year} — output already exists")
                    else:
                        in_file = os.path.join(BASE_SIM_DIR, casename,
                                               IN_RELPATH, get_in_file(year, year_start, year_final))
                        if os.path.exists(in_file):
                            calc(
                                in_file=in_file,
                                out_file=out_file,
                                start_time=f"{year}-01-01",
                                end_time=f"{year}-12-31"
                            )
                        else:
                            print(f"[MISSING] {casename}, {year} — input file does not exist")
                        continue


if __name__ == "__main__":
    main()
