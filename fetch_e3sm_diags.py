# -*- coding: utf-8 -*-
"""
fetch_e3sm_diags.py

Download the ANN_metrics_table.csv produced by e3sm_diags for each XLE
historical ensemble member and place it under the simulation's post directory
on spinning disk.

Target path for each member:
    <BASE_OUTPUT_DIR>/<sim_name>/post/e3sm_diags/ANN_metrics_table.csv

Workflow:
  1. For each simulation in SIMULATIONS, check whether the CSV already exists.
  2. Skip files already present.
  3. Otherwise, download to a .tmp file first; rename to the final path only
     on success (atomic write).  The .tmp file is always cleaned up on exit.

Run:   python fetch_e3sm_diags.py
Test:  set DRY_RUN = True to print download URLs without fetching anything.
"""

import socket
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Runtime control
# ---------------------------------------------------------------------------

DRY_RUN = False  # Set to False to actually download files

# Global socket timeout (seconds) — prevents hung downloads from stalling
# the entire run.
socket.setdefaulttimeout(60)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Root directory on spinning disk.  Each simulation lands in its own subdir.
BASE_OUTPUT_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")

# Path fragment appended to each simulation's parent web URL to reach the CSV.
CSV_SUFFIX = (
    "e3sm_diags/atm_monthly_180x360_aave/model_vs_obs_1985-2014"
    "/viewer/table-data/ANN_metrics_table.csv"
)

# Destination sub-path within each simulation's local directory.
DEST_SUBDIR = Path("post/e3sm_diags")
DEST_FILENAME = "ANN_metrics_table.csv"

# ---------------------------------------------------------------------------
# Simulation registry
# ---------------------------------------------------------------------------
# Flat dict: sim_name -> base_url (trailing slash optional).
# Members with no known public URL are omitted.
#
# Source: E3SM Diags / ILAMB / MPAS-Analysis Locations Confluence table
#   https://e3sm.atlassian.net/wiki/spaces/WCCI/pages/6383894554/

SIMULATIONS = {

    # -----------------------------------------------------------------------
    # v3.LR.historical  (25 members — all via ac.wlin on lcrc)
    # -----------------------------------------------------------------------
    "v3.LR.historical_0051": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0051/",
    "v3.LR.historical_0091": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0091/",
    "v3.LR.historical_0101": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0101/",
    "v3.LR.historical_0111": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0111/",
    "v3.LR.historical_0121": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0121/",
    "v3.LR.historical_0131": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0131/",
    "v3.LR.historical_0141": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0141/",
    "v3.LR.historical_0151": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0151/",
    "v3.LR.historical_0161": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0161/",
    "v3.LR.historical_0171": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0171/",
    "v3.LR.historical_0181": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0181/",
    "v3.LR.historical_0191": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0191/",
    "v3.LR.historical_0201": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0201/",
    "v3.LR.historical_0211": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0211/",
    "v3.LR.historical_0221": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0221/",
    "v3.LR.historical_0231": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0231/",
    "v3.LR.historical_0241": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0241/",
    "v3.LR.historical_0251": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0251/",
    "v3.LR.historical_0261": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0261/",
    "v3.LR.historical_0271": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0271/",
    "v3.LR.historical_0281": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0281/",
    "v3.LR.historical_0291": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0291/",
    "v3.LR.historical_0301": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0301/",
    "v3.LR.historical_0311": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0311/",
    "v3.LR.historical_0321": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.wlin/E3SMv3/v3.LR.historical_0321/",

    # -----------------------------------------------------------------------
    # v3.LR.lowECS.historical  (25 members with known URLs)
    # Omitted (no public URL): 0261
    # -----------------------------------------------------------------------

    # --- harr152 on compy-dtn.pnl.gov (15 members) ---
    "v3.LR.lowECS.historical_0051": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0051/",
    "v3.LR.lowECS.historical_0091": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0091/",
    "v3.LR.lowECS.historical_0101": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0101/",
    "v3.LR.lowECS.historical_0131": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0131/",
    "v3.LR.lowECS.historical_0141": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0141/",
    "v3.LR.lowECS.historical_0151": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0151/",
    "v3.LR.lowECS.historical_0161": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0161/",
    "v3.LR.lowECS.historical_0171": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0171/",
    "v3.LR.lowECS.historical_0181": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0181/",
    "v3.LR.lowECS.historical_0191": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0191/",
    "v3.LR.lowECS.historical_0201": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0201/",
    "v3.LR.lowECS.historical_0211": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0211/",
    "v3.LR.lowECS.historical_0221": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0221/",
    "v3.LR.lowECS.historical_0241": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.lowECS.historical_0241/",
    

    # --- ac.bharrop on lcrc (2 members) ---
    "v3.LR.lowECS.historical_0111": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.bharrop/E3SMv3/v3.LR.lowECS.historical_0111/",
    "v3.LR.lowECS.historical_0121": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.bharrop/E3SMv3/v3.LR.lowECS.historical_0121/",

    # --- ac.claudia.tebaldi on lcrc (6 members) ---
    "v3.LR.lowECS.historical_0271": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0271/",
    "v3.LR.lowECS.historical_0281": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0281/",
    "v3.LR.lowECS.historical_0291": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0291/",
    "v3.LR.lowECS.historical_0301": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0301/",
    "v3.LR.lowECS.historical_0311": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0311/",
    "v3.LR.lowECS.historical_0321": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.claudia.tebaldi/E3SMv3/v3.LR.lowECS.historical_0321/",

    # --- ac.smahajan on lcrc (1 member) ---
    "v3.LR.lowECS.historical_0231": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.smahajan/E3SMv3/v3.LR.lowECS.historical_0231/",

    # --- ac.kzhang on lcrc (2 members) ---
    "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.kzhang/E3SMv3/v3.LR.lowECS.historical_0251/",
    "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.kzhang/E3SMv3/v3.LR.lowECS.historical_0261/",


    # -----------------------------------------------------------------------
    # v3.LR.highECS.historical  (25 members with known URLs)
    # -----------------------------------------------------------------------

    # --- harr152 on compy-dtn.pnl.gov (9 members) ---
    "v3.LR.highECS.historical_0051": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0051/",
    "v3.LR.highECS.historical_0091": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0091/",
    "v3.LR.highECS.historical_0101": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0101/",
    "v3.LR.highECS.historical_0271": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0271/",
    "v3.LR.highECS.historical_0281": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0281/",
    "v3.LR.highECS.historical_0291": "https://web.lcrc.anl.gov/public/e3sm/diagnostic_output/ac.bharrop/E3SMv3/v3.LR.highECS.historical_0291/",
    "v3.LR.highECS.historical_0301": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0301/",
    "v3.LR.highECS.historical_0311": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0311/",
    "v3.LR.highECS.historical_0321": "https://compy-dtn.pnl.gov/harr152/E3SMv3/v3.LR.highECS.historical_0321/",

    # --- teba502 on compy-dtn.pnl.gov (16 members) ---
    "v3.LR.highECS.historical_0111": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0111/",
    "v3.LR.highECS.historical_0121": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0121/",
    "v3.LR.highECS.historical_0131": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0131/",
    "v3.LR.highECS.historical_0141": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0141/",
    "v3.LR.highECS.historical_0151": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0151/",
    "v3.LR.highECS.historical_0161": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0161/",
    "v3.LR.highECS.historical_0171": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0171/",
    "v3.LR.highECS.historical_0181": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0181/",
    "v3.LR.highECS.historical_0191": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0191/",
    "v3.LR.highECS.historical_0201": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0201/",
    "v3.LR.highECS.historical_0211": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0211/",
    "v3.LR.highECS.historical_0221": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0221/",
    "v3.LR.highECS.historical_0231": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0231/",
    "v3.LR.highECS.historical_0241": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0241/",
    "v3.LR.highECS.historical_0251": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0251/",
    "v3.LR.highECS.historical_0261": "https://compy-dtn.pnl.gov/teba502/E3SMv3/v3.LR.highECS.historical_0261/",
}

# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def fetch_csv(sim_name: str, base_url: str) -> None:
    """Download ANN_metrics_table.csv for one simulation.

    Downloads to a .tmp file first; renames to the final path only on
    success.  The .tmp file is always removed on exit, whether the download
    succeeded, failed, or was interrupted.
    """
    dest_path = BASE_OUTPUT_DIR / sim_name / DEST_SUBDIR / DEST_FILENAME
    tmp_path = dest_path.with_suffix(".tmp")
    url = base_url.rstrip("/") + "/" + CSV_SUFFIX

    if dest_path.exists():
        print(f"  [present]  {dest_path}")
        return

    if DRY_RUN:
        print(f"  [would download]  {url}")
        print(f"               ->  {dest_path}")
        return

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  [downloading]  {url}")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0"},
    )
    try:
        with urllib.request.urlopen(req) as response, open(tmp_path, "wb") as fh:
            fh.write(response.read())
        tmp_path.replace(dest_path)
        print(f"  [saved]        {dest_path}")
    except (urllib.error.URLError, OSError) as exc:
        print(f"  [ERROR]        {sim_name}: {exc}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if DRY_RUN:
        print("=== DRY RUN — no files will be downloaded ===\n")

    for sim_name, base_url in SIMULATIONS.items():
        print(f"\n--- {sim_name} ---")
        fetch_csv(sim_name, base_url)

    print("\nDone.")


if __name__ == "__main__":
    main()
