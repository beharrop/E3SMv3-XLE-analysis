"""Generate ETCCDI processing scripts for E3SM ensemble cases.

Run ``python submit_etccdi_e3sm_scripts.py`` to generate scripts, or add
``--submit`` to submit generated scripts with sbatch.
"""

import argparse
import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR   = Path(__file__).parent
TEMPLATE_SH  = SCRIPT_DIR / "run_etccdi_e3sm_template.sh"
TEMP_SCRIPTS = SCRIPT_DIR / "temp_scripts"
ETCCDI_OUTPUT_DIR = Path("/pscratch/sd/b/beharrop/E3SMv3/XLE/ETCCDI")
EXPERIMENTS = (
    "v3.LR.historical",
    "v3.LR.highECS.historical",
    "v3.LR.lowECS.historical",
)

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

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--submit",
        action="store_true",
        help="submit generated scripts with sbatch",
    )
    args = parser.parse_args(argv)

    TEMP_SCRIPTS.mkdir(parents=True, exist_ok=True)
    template_text = TEMPLATE_SH.read_text()

    generated = 0
    submitted = 0

    for experiment in EXPERIMENTS:
        for member_id in ENSEMBLE_MEMBERS:
            casename = f"{experiment}_{member_id}"
            results_file = ETCCDI_OUTPUT_DIR / casename / "results" / "E3SM_etccdi.nc"

            if results_file.exists():
                print(f"[SKIP]    {casename} — combined result already exists")
                continue

            bash_text = template_text.replace("{casename}", casename)
            bash_file = TEMP_SCRIPTS / f"run_get_etccdi_{casename}.sh"
            bash_file.write_text(bash_text)
            generated += 1
            print(f"[WROTE]   {bash_file.name}")

            if args.submit:
                try:
                    result = subprocess.run(
                        ["sbatch", str(bash_file)],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        print(f"[ERROR]   sbatch failed for {casename}: {result.stderr.strip()}")
                    else:
                        submitted += 1
                        print(f"[SUBMITTED] {casename} — {result.stdout.strip()}")
                except OSError as exc:
                    print(f"[ERROR]   could not submit {casename}: {exc}")

    print(f"\nDone. Generated {generated} script(s); submitted {submitted} job(s).")


if __name__ == "__main__":
    main()
