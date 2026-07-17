"""submit_rainrate_processing_scripts.py

Loop through the ensemble members.  For each member that does not yet have:

    /global/cfs/cdirs/e3smdata/simulations/v3.XLE/v3.LR.highECS.historical_{ensemble}/
        post/atm/native/rainhist/daily/1yr/RRC_2014.nc

write a bash script file to temp_scripts/ (from the template) and submit it 
to the queue via sbatch temp_scripts/{script_name}.

Run:  python submit_rainrate_processing_scripts.py
"""

import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR   = Path(__file__).parent
TEMPLATE_CFG = SCRIPT_DIR / "run_process_daily_rainrates_template.sh"
TEMP_SCRIPTS = SCRIPT_DIR / "temp_scripts"
BASE_SIM_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")
CHECK_RELPATH = "post/atm/native/rainhist/daily/1yr/RRC_2014.nc"

# ---------------------------------------------------------------------------
# Ensemble member registry  (mirrors manage_data.py _ENSEMBLE_MEMBERS)
# ---------------------------------------------------------------------------

ENSEMBLE_MEMBERS = [
        "0051", "0091", "0101", "0111", "0121",
        "0131", "0141", "0151", "0161", "0171",
        "0181", "0191", "0201", "0211", "0221",
        "0231", "0241", "0251", "0261", "0271",
        "0281", "0291", "0301", "0311", "0321",
    ]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    TEMP_SCRIPTS.mkdir(parents=True, exist_ok=True)
    template_text = TEMPLATE_CFG.read_text()

    launched = []

    for member_id in ENSEMBLE_MEMBERS:
        casename   = f"v3.LR.highECS.historical_{member_id}"
        check_path = BASE_SIM_DIR / casename / CHECK_RELPATH

        if check_path.exists():
            print(f"[SKIP]    {member_id} — output already exists")
            continue

        # Write per-ensemble bash script (always overwrite)
        bash_text = template_text.replace("{ensemble}", member_id)
        bash_file = TEMP_SCRIPTS / f"run_process_daily_rainrates_{member_id}.sh"
        bash_file.write_text(bash_text)
        print(f"[bash_script]     wrote {bash_file.name}")

        # Launch sbatch (fire-and-forget)
        try:
            result = subprocess.run(
                ["sbatch", str(bash_file)],
                capture_output=True, text=True
            )
            job_id = result.stdout.strip()  # "Submitted batch job 12345"
            if result.returncode != 0:
                print(f"[ERROR]   sbatch failed for {member_id}: {result.stderr.strip()}")
            else:
                print(f"[LAUNCH]  {member_id} → {job_id}")
                launched.append(member_id)
        except Exception as exc:
            print(f"[ERROR]   could not launch sbatch for {member_id}: {exc}")

    print(f"\nDone. Launched {len(launched)} sbatch job(s) in the background.")


if __name__ == "__main__":
    main()
