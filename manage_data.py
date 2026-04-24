"""
manage_data.py

Pull E3SM simulation data from HPSS tape to spinning disk.

Workflow:
  1. For each simulation in SIMULATIONS, resolve which variables to pull
     based on the simulation's group tag.
  2. Construct expected filenames from simulation metadata and analysis dates.
  3. Check local disk — skip files already present.
  4. Batch all missing files for a simulation into a single zstash call.

Run:   python manage_data.py
Test:  set DRY_RUN = True to print zstash commands without executing them.
"""

import os
import re
import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Runtime control
# ---------------------------------------------------------------------------

DRY_RUN = True  # Set to False to actually pull data from tape

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Root directory on spinning disk. Each simulation lands in its own subdir.
BASE_OUTPUT_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")

# Default variable list — used when a simulation's group has no specific entry.
# Each entry is a dict with the following keys:
#   name      (str) : variable name
#   timescale (str) : "monthly" or "daily"
#   domain    (str) : "atm", "lnd", or "rof"
#   grid      (str) : one of "180x360_aave", "native", "glb",
#                          "MDA8EU1.0x1.0_nco", "MDA8US1.0x1.0_nco"
DEFAULT_VARIABLES = [
    # {"name": "TREFHT", "timescale": "monthly", "domain": "atm", "grid": "180x360_aave"},
    # TODO: populate
    #{'name': 'PRECT', 'timescale': 'daily', 'domain': 'atm', 'grid': '180x360_aave'},
]

# Per-group variable lists. Key is the group tag used in SIMULATIONS entries.
# Any group not listed here will fall back to DEFAULT_VARIABLES.
GROUP_VARIABLES = {
    # "group_tag": [
    #     {"name": "var1", "timescale": "monthly", "domain": "atm", "grid": "180x360_aave"},
    #     {"name": "var2", "timescale": "daily",   "domain": "lnd", "grid": "native"},
    # ],
    # TODO: populate groups and their variable lists
    "piControl": [
        {"filepath": "post/atm/glb/ts/monthly/10yr/FSNTOA_{start}_{end}.nc", "start": "000101", "end": "050012"},
        {"filepath": "post/atm/glb/ts/monthly/10yr/FLUT_{start}_{end}.nc",   "start": "000101", "end": "050012"},
        {"filepath": "post/atm/glb/ts/monthly/10yr/TREFHT_{start}_{end}.nc", "start": "000101", "end": "050012"},
        {"filepath": "post/atm/glb/ts/monthly/10yr/PRECC_{start}_{end}.nc",  "start": "000101", "end": "050012"},
        {"filepath": "post/atm/glb/ts/monthly/10yr/PRECL_{start}_{end}.nc",  "start": "000101", "end": "050012"},
    ],
    "historical": [
        {"filepath": "post/atm/glb/ts/monthly/5yr/FSNTOA_{start}_{end}.nc", "start": "185001", "end": "202412"},
        {"filepath": "post/atm/glb/ts/monthly/5yr/FLUT_{start}_{end}.nc",   "start": "185001", "end": "202412"},
        {"filepath": "post/atm/glb/ts/monthly/5yr/TREFHT_{start}_{end}.nc", "start": "185001", "end": "202412"},
        {"filepath": "post/atm/glb/ts/monthly/5yr/PRECC_{start}_{end}.nc",  "start": "185001", "end": "202412"},
        {"filepath": "post/atm/glb/ts/monthly/5yr/PRECL_{start}_{end}.nc",  "start": "185001", "end": "202412"},
        {"filepath": "archive/atm/hist/v3.LR.lowECS.historical_{ens}.eam.h1.{YYYY}-{MM}-{DD}-00000.nc", "start": "1985", "end": "2024"},
    ],
    "ssp370": [
        {"filepath": "post/atm/glb/ts/monthly/4yr/FSNTOA_{start}_{end}.nc", "start": "202501", "end": "210012"},
        {"filepath": "post/atm/glb/ts/monthly/4yr/FLUT_{start}_{end}.nc",   "start": "202501", "end": "210012"},
        {"filepath": "post/atm/glb/ts/monthly/4yr/TREFHT_{start}_{end}.nc", "start": "202501", "end": "210012"},
        {"filepath": "post/atm/glb/ts/monthly/4yr/PRECC_{start}_{end}.nc",  "start": "202501", "end": "210012"},
        {"filepath": "post/atm/glb/ts/monthly/4yr/PRECL_{start}_{end}.nc",  "start": "202501", "end": "210012"},
        
    ]
}

# ---------------------------------------------------------------------------
# Simulation registry
# ---------------------------------------------------------------------------

# Non-ensemble (single-run) simulations.
# Each entry:
#   hpss_path      (str)        : Path to the zstash archive on HPSS
#   analysis_dates (tuple[str]) : (start, end) date strings for filename construction
#                                 TODO: confirm format (e.g. "YYYYMM", "YYYY-MM", ...)
#   group          (str)        : Tag that keys into GROUP_VARIABLES
_SINGLE_SIMULATIONS = {
    "v3.LR.piControl": {
        "hpss_path": "/home/g/golaz/E3SMv3.LR/v3.LR.piControl/",
        "analysis_dates": ("000101", "050012"),
        "group": "piControl",
    },
    "v3.LR.lowECS.piControl": {
        "hpss_path": "/home/b/beharrop/E3SMv3/v3.LR.lowECS/v3.LR.lowECS.piControl",
        "analysis_dates": ("000101", "050012"),
        "group": "piControl",
    },
    "v3.LR.highECS.piControl": {
        "hpss_path": "/home/b/beharrop/E3SMv3/v3.LR.highECS/v3.LR.highECS.piControl",
        "analysis_dates": ("000101", "050012"),
        "group": "piControl",
    },
}

# Shared settings (group, analysis_dates) per ensemble experiment.
# Keys match those used in _ENSEMBLE_MEMBERS below.
# Commented out ssp370 runs for now since we are not using those
_ENSEMBLE_DEFAULTS = {
    "v3.LR.historical": {
        "group": "historical",
        "analysis_dates": ("185001", "201412"),
    },
    "v3.LR.lowECS.historical": {
        "group": "historical",
        "analysis_dates": ("185001", "201412"),
    },
    "v3.LR.highECS.historical": {
        "group": "historical",
        "analysis_dates": ("185001", "201412"),
    },
    # "v3.LR.ssp370": {
    #     "group": "ssp370",
    #     "analysis_dates": ("202501", "210012"),
    # },
    # "v3.LR.lowECS.ssp370": {
    #     "group": "ssp370",
    #     "analysis_dates": ("202501", "210012"),
    # },
    # "v3.LR.highECS.ssp370": {
    #     "group": "ssp370",
    #     "analysis_dates": ("202501", "210012"),
    # },
}

# Per-experiment member groups. Members run by the same user share an HPSS
# path structure and are collected into one dict with:
#   hpss_template (str)       : HPSS archive path with {member_id} placeholder
#   members       (list[str]) : Member IDs that belong to this user/path structure
# Add additional user-group dicts to each experiment list as needed.
_ENSEMBLE_MEMBERS = {
    "v3.LR.historical": [
        {
            "hpss_template": "/home/w/wlin/E3SMv3/v3.LR.historical_{member_id}",
            "members": ["0051", "0091", "0101", "0111", "0121",
                        "0131", "0141", "0151", "0161", "0171",
                        "0181", "0191", "0201", "0211", "0221",
                        "0231", "0241", "0251", "0261", "0271",
                        "0281", "0291", "0301", "0311", "0321"], 
        },
        # {
        #     "hpss_template": "/home/x/other_user/E3SMv3/v3.LR.historical_{member_id}",
        #     "members": ["0001", "0002", ...],
        # },
    ],

    "v3.LR.lowECS.historical": [
        {
            "hpss_template": "/home/b/beharrop/E3SMv3/v3.LR.lowECS/v3.LR.lowECS.historical_{member_id}",
            "members": ["0051", "0091", "0101", "0111", "0121", 
                        "0131", "0141", "0151", "0161", "0171", 
                        "0181", "0191", "0201", "0211"], 
        },
        {
            "hpss_template": "/home/k/kaizhang/E3SM/E3SMv3/v3.LR.lowECS/v3.LR.lowECS.historical_{member_id}",
            "members": [],
        },
    ],

    "v3.LR.highECS.historical": [
        {
            "hpss_template": "/home/b/beharrop/E3SMv3/v3.LR.highECS/v3.LR.highECS.historical_{member_id}",
            "members": ["0051"], 
        },
    ],

    # "v3.LR.ssp370": [
    #     {
    #         "hpss_template": "/home/b/beharrop/E3SMv3/v3.LR/v3.LR.ssp370_{member_id}",
    #         "members": ["0051", "0101", "0151", "0201", "0251"], 
    #     },
    #     {
    #         "hpss_template": "/home/k/kaizhang/E3SM/E3SMv3/v3.LR/v3.LR.ssp370_{member_id}",
    #         "members": [], 
    #     }
    # ],

    # "v3.LR.lowECS.ssp370": [
    #     {
    #         "hpss_template": "/home/b/beharrop/E3SMv3/v3.LR.lowECS/v3.LR.lowECS.ssp370_{member_id}",
    #         "members": ["0051", "0101", "0151", "0201", "0251"], 
    #     },
    #     {
    #         "hpss_template": "/home/k/kaizhang/E3SM/E3SMv3/v3.LR.lowECS/v3.LR.lowECS.ssp370_{member_id}",
    #         "members": [], 
    #     }
    # ],

    # "v3.LR.highECS.ssp370": [
    #     {
    #         "hpss_template": "/home/b/beharrop/E3SMv3/v3.LR.highECS/v3.LR.highECS.ssp370_{member_id}",
    #         "members": ["0051", "0101", "0151", "0201", "0251"], 
    #     },
    #     {
    #         "hpss_template": "/home/k/kaizhang/E3SM/E3SMv3/v3.LR.highECS/v3.LR.highECS.ssp370_{member_id}",
    #         "members": [], 
    #     }
    # ],
}


def _expand_simulations() -> dict:
    """Flatten _ENSEMBLE_MEMBERS into individual simulation entries.

    Each entry is keyed as "{experiment}_{member_id}" and inherits the shared
    settings from _ENSEMBLE_DEFAULTS, with hpss_path filled in from the
    user-group template.
    """
    expanded = {}
    for experiment, user_groups in _ENSEMBLE_MEMBERS.items():
        defaults = _ENSEMBLE_DEFAULTS[experiment]
        for user_group in user_groups:
            template = user_group["hpss_template"]
            for member_id in user_group["members"]:
                sim_name = f"{experiment}_{member_id}"
                expanded[sim_name] = {
                    **defaults,
                    "hpss_path": template.format(member_id=member_id),
                }
    return expanded


# Full simulation registry: single runs merged with expanded ensemble members.
SIMULATIONS = {**_SINGLE_SIMULATIONS, **_expand_simulations()}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_variables(sim_config: dict) -> list:
    """Return the variable list for a simulation based on its group tag.

    Falls back to DEFAULT_VARIABLES if the group has no specific entry.
    """
    group = sim_config.get("group", "")
    return GROUP_VARIABLES.get(group, DEFAULT_VARIABLES)


def _parse_segment_years(filepath: str) -> int:
    """Extract the segment duration in years from a path component like '5yr'."""
    match = re.search(r'/(\d+)yr/', filepath)
    if match:
        return int(match.group(1))
    raise ValueError(f"Could not parse segment size from filepath: {filepath}")


def _iter_segments(start: str, end: str, segment_years: int):
    """Yield (seg_start, seg_end) YYYYMM string pairs covering start..end.

    Each segment spans exactly segment_years years, beginning in the month
    given by start and ending in December of the last year of that segment.
    """
    yr = int(start[:4])
    mo = int(start[4:6])
    end_yr = int(end[:4])
    end_mo = int(end[4:6])

    while (yr, mo) <= (end_yr, end_mo):
        seg_end_yr = yr + segment_years - 1
        yield f"{yr:04d}{mo:02d}", f"{seg_end_yr:04d}12"
        yr = seg_end_yr + 1
        mo = 1  # subsequent segments always start in January


def _get_member_id(sim_name: str) -> str:
    """Extract the trailing member ID from a sim name like 'exp_0051' → '0051'.

    Returns the full sim_name unchanged if no underscore-delimited suffix is present.
    """
    parts = sim_name.rsplit("_", 1)
    return parts[1] if len(parts) == 2 else sim_name


def _is_daily_archive(filepath_template: str) -> bool:
    """Return True when the filepath template uses {YYYY}/{MM}/{DD} date placeholders."""
    return "{YYYY}" in filepath_template


def _build_daily_archive_year_glob(filepath_template: str, year: int, member_id: str) -> str:
    """Return a glob pattern matching all daily archive files for *year*."""
    pattern = filepath_template.replace("{ens}", member_id)
    pattern = pattern.replace("{YYYY}", f"{year:04d}")
    pattern = pattern.replace("{MM}", "*")
    pattern = pattern.replace("{DD}", "*")
    return pattern


def _daily_archive_to_pull(sim_name: str, sim_config: dict, var: dict) -> list:
    """Return missing glob patterns for a daily archive variable.

    Covers one glob pattern per year in [start, end] plus a glob for all
    files from the preceding year (to provide boundary data for start_year).
    """
    member_id = _get_member_id(sim_name)
    base_dir = _get_base_dir(sim_config) / sim_name

    start_year = int(var["start"])
    end_year = int(var["end"])

    missing = []

    # --- one glob pattern per year in [start, end] ---
    for year in range(start_year, end_year + 1):
        pattern = _build_daily_archive_year_glob(var["filepath"], year, member_id)
        local_matches = sorted(base_dir.glob(pattern))
        if local_matches:
            print(f"  [present]  {len(local_matches)} file(s) for year {year}")
        else:
            print(f"  [missing]  {pattern}")
            missing.append(pattern)

    # --- all files from the preceding year (provides boundary data for start_year) ---
    preceding_year = start_year - 1
    preceding_glob = _build_daily_archive_year_glob(var["filepath"], preceding_year, member_id)
    local_preceding = sorted(base_dir.glob(preceding_glob))
    if local_preceding:
        print(f"  [present]  {len(local_preceding)} file(s) for year {preceding_year}")
    else:
        print(f"  [missing]  {preceding_glob}")
        missing.append(preceding_glob)

    return missing


def build_filenames(sim_name: str, sim_config: dict, variables: list) -> list:
    """Construct the expected filenames for a simulation.

    Each entry in *variables* is a dict with keys:
      filepath  (str) : relative path template containing {start} and {end}
                        placeholders and a segment-size component (e.g. '5yr')
      start     (str) : overall first date of the timeseries (YYYYMM)
      end       (str) : overall last  date of the timeseries (YYYYMM)

    The segment size is parsed from the filepath (e.g. '5yr' → 5 years).
    One filename is produced per segment per variable.
    """
    filenames = []
    for var in variables:
        if _is_daily_archive(var["filepath"]):
            continue  # daily archive vars are handled by _daily_archive_to_pull
        filepath_template = var["filepath"]
        seg_years = _parse_segment_years(filepath_template)
        for seg_start, seg_end in _iter_segments(var["start"], var["end"], seg_years):
            filenames.append(filepath_template.format(start=seg_start, end=seg_end))
    return filenames


def _get_base_dir(sim_config: dict) -> Path:
    """Return the output base directory for a simulation.

    piControl runs land directly under the simulations root (no v3.XLE subdir).
    All other groups use BASE_OUTPUT_DIR.
    """
    if sim_config.get("group") == "piControl":
        return BASE_OUTPUT_DIR.parent
    return BASE_OUTPUT_DIR


def get_local_path(sim_name: str, sim_config: dict, filename: str) -> Path:
    """Return the expected local path for a file."""
    return _get_base_dir(sim_config) / sim_name / filename


def files_to_pull(sim_name: str, sim_config: dict) -> list:
    """Return a list of filenames/patterns that are missing from local disk."""
    variables = get_variables(sim_config)
    missing = []

    for var in variables:
        if _is_daily_archive(var["filepath"]):
            missing.extend(_daily_archive_to_pull(sim_name, sim_config, var))
        else:
            seg_years = _parse_segment_years(var["filepath"])
            for seg_start, seg_end in _iter_segments(var["start"], var["end"], seg_years):
                fname = var["filepath"].format(start=seg_start, end=seg_end)
                local_path = get_local_path(sim_name, sim_config, fname)
                if local_path.exists():
                    print(f"  [present]  {local_path}")
                else:
                    print(f"  [missing]  {local_path}")
                    missing.append(fname)

    return missing


def pull_files(sim_name: str, sim_config: dict, missing_files: list):
    """Run a single zstash command to pull all missing files for a simulation.

    TODO: implement once zstash syntax is provided.
          Expected inputs:
            - sim_config["hpss_path"] : path to the HPSS archive
            - missing_files           : list of filenames to extract
    """
    hpss_path = sim_config["hpss_path"]
    output_dir = _get_base_dir(sim_config) / sim_name
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["zstash", "extract", f"--hpss={hpss_path}"] + missing_files

    print(f"  zstash command: {' '.join(str(c) for c in cmd)}")
    if not DRY_RUN:
        subprocess.run(cmd, check=True, cwd=output_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if DRY_RUN:
        print("=== DRY RUN — no data will be pulled ===\n")

    if not SIMULATIONS:
        print("No simulations configured in SIMULATIONS. Nothing to do.")
        return

    for sim_name, sim_config in SIMULATIONS.items():
        print(f"\n--- {sim_name} {sim_config}---")
        missing = files_to_pull(sim_name, sim_config)

        if not missing:
            print(f"  All files already present. Skipping.")
            continue

        print(f"  {len(missing)} file(s) to pull.")
        pull_files(sim_name, sim_config, missing)

    print("\nDone.")


if __name__ == "__main__":
    main()
