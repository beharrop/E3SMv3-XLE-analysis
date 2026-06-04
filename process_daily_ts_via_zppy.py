"""process_daily_ts_via_zppy.py

Loop through the v3.LR.historical, v3.LR.lowECS.historical, and
v3.LR.highECS.historical ensembles.  For each member that does not yet have:

    /global/cfs/cdirs/e3smdata/simulations/v3.XLE/{casename}/
        post/atm/180x360_aave/ts/daily/5yr/TREFHT_202001_202412.nc

write a per-case cfg file to temp_scripts/ (from the template) and launch
`zppy` as a non-blocking subprocess to set up the post-processing steps.

Run:  python process_daily_ts_via_zppy.py
"""

import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR   = Path(__file__).parent
TEMPLATE_CFG = SCRIPT_DIR / "daily_timeseries_zppy_template.cfg"
TEMP_SCRIPTS = SCRIPT_DIR / "temp_scripts"
BASE_SIM_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")
CHECK_RELPATH = "post/atm/180x360_aave/ts/daily/5yr/TREFHT_202001_202412.nc"

# ---------------------------------------------------------------------------
# Ensemble member registry  (mirrors manage_data.py _ENSEMBLE_MEMBERS)
# ---------------------------------------------------------------------------

ENSEMBLE_MEMBERS = {
    "v3.LR.historical": [
        "0051", "0091", "0101", "0111", "0121",
        "0131", "0141", "0151", "0161", "0171",
        "0181", "0191", "0201", "0211", "0221",
        "0231", "0241", "0251", "0261", "0271",
        "0281", "0291", "0301", "0311", "0321",
    ],
    "v3.LR.lowECS.historical": [
        "0051", "0091", "0101", "0111", "0121",
        "0131", "0141", "0151", "0161", "0171",
        "0181", "0191", "0201", "0211", "0221",
        "0291", "0301", "0311", "0321",
    ],
    "v3.LR.highECS.historical": [
        "0051", "0091", "0101", "0111", "0121",
        "0131", "0141", "0151", "0161", "0171",
        "0181", "0191", "0201", "0211", "0221",
        "0231", "0281", "0301", "0311", "0321",
    ],
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    TEMP_SCRIPTS.mkdir(parents=True, exist_ok=True)
    template_text = TEMPLATE_CFG.read_text()

    launched = []

    for ensemble, members in ENSEMBLE_MEMBERS.items():
        for member_id in members:
            casename   = f"{ensemble}_{member_id}"
            check_path = BASE_SIM_DIR / casename / CHECK_RELPATH

            if check_path.exists():
                print(f"[SKIP]    {casename} — output already exists")
                continue

            # Write per-case cfg (always overwrite)
            cfg_text = template_text.replace("{casename}", casename)
            cfg_file = TEMP_SCRIPTS / f"daily_timeseries_zppy_{casename}.cfg"
            cfg_file.write_text(cfg_text)
            print(f"[CFG]     wrote {cfg_file.name}")

            # Launch zppy (fire-and-forget)
            try:
                proc = subprocess.Popen(
                    ["zppy", "-c", str(cfg_file)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                launched.append((casename, proc))
                print(f"[LAUNCH]  zppy pid={proc.pid} — {casename}")
            except Exception as exc:
                print(f"[ERROR]   could not launch zppy for {casename}: {exc}")

    print(f"\nDone. Launched {len(launched)} zppy job(s) in the background.")


if __name__ == "__main__":
    main()
