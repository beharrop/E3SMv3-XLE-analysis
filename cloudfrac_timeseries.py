import xarray as xr
import pandas as pd
import numpy as np
import os
import re
import pyproj
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.lines as mlines
import metpy
import datetime
import cartopy.crs as ccrs
import cartopy.feature as cf
from functools import partial
import xesmf as xe
import gc
import warnings
import cftime
from scipy import stats
import seaborn as sns
import glob
import yaml
from pathlib import Path
from itertools import combinations
import xcdat
import xclim
import pdb
from typing import List, Tuple

# ── Configuration ─────────────────────────────────────────────────────────────
XLE_PARENT_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")

# All 25 target member IDs (same across all three large ensembles)
MEMBER_IDS = [
    "0051", "0091", "0101", 
    "0111", "0121", "0131", "0141", "0151",
    "0161", "0171", "0181", "0191", "0201", "0211", "0221", "0231",
    "0241", "0251", "0261", "0271", "0281", "0291", "0301", "0311", "0321",
]

# Map short name → directory prefix for each large ensemble
ENSEMBLE_PREFIXES = {
    "default": "v3.LR.historical",
    "highECS": "v3.LR.highECS.historical",
    "lowECS":  "v3.LR.lowECS.historical",
}

# Variables to load
VARIABLES = ["TREFHT", "PRECC", "PRECL", "CLDLOW", "CLDMED", "CLDHGH"]

def load_large_ensemble(prefix, member_ids, variables, parent_dir):
    """
    Load monthly timeseries data for one E3SM large ensemble.

    For each member in `member_ids` the function globs all 5-yr NetCDF chunks
    under  <parent_dir>/<prefix>_<member_id>/post/atm/glb/ts/monthly/5yr/
    for every variable in `variables`, concatenates them along the time
    dimension, and stacks the result along a new `ensemble` dimension.

    A member is silently skipped if *any* requested variable has no matching
    files, keeping the returned Dataset complete and rectangular.

    Parameters
    ----------
    prefix : str
        Directory prefix for the ensemble, e.g. "v3.LR.highECS.historical".
    member_ids : list of str
        Candidate member IDs to attempt loading (e.g. ["0051", "0091", ...]).
    variables : list of str
        Variable names to load (e.g. ["TREFHT", "PRECC", "PRECL"]).
    parent_dir : Path
        Root directory containing all ensemble member subdirectories.

    Returns
    -------
    ds : xr.Dataset or None
        Dataset with dimensions (ensemble, time, rgn).  Returns None if no
        members could be loaded.
    loaded_members : list of str
        Member IDs that were successfully loaded.
    """
    member_datasets = []
    loaded_members  = []

    for member_id in member_ids:
        ts_dir = (
            parent_dir
            / f"{prefix}_{member_id}"
            / "post" / "atm" / "glb" / "ts" / "monthly" / "5yr"
        )

        # Collect all time-chunk files for each variable
        var_data_arrays = {}
        all_vars_present = True

        for var in variables:
            files = sorted(ts_dir.glob(f"{var}_*.nc"))
            if not files:
                all_vars_present = False
                break
            # Concatenate any multiple 5-yr chunks along the time axis
            ds_var = xr.open_mfdataset(
                files,
                combine="nested",
                concat_dim="time",
                data_vars="minimal",
                coords="minimal",
                compat="override",
            )
            var_data_arrays[var] = ds_var[var]

        if not all_vars_present:
            print(f"  [SKIP] {prefix}_{member_id}: one or more variables absent")
            continue

        member_datasets.append(xr.Dataset(var_data_arrays))
        loaded_members.append(member_id)

    if not member_datasets:
        print(f"  [WARNING] No members loaded for {prefix}")
        return None, []

    ds_ensemble = xr.concat(member_datasets, dim="ensemble")
    ds_ensemble = ds_ensemble.assign_coords(ensemble=loaded_members)

    return ds_ensemble, loaded_members


# ── Load all three large ensembles ────────────────────────────────────────────
print("Loading v3.LR.historical …")
ds_default, members_default = load_large_ensemble(
    ENSEMBLE_PREFIXES["default"], MEMBER_IDS, VARIABLES, XLE_PARENT_DIR
)
n_members_default = len(members_default)
print(f"  {n_members_default} / {len(MEMBER_IDS)} members loaded\n")

print("Loading v3.LR.highECS.historical …")
ds_highECS, members_highECS = load_large_ensemble(
    ENSEMBLE_PREFIXES["highECS"], MEMBER_IDS, VARIABLES, XLE_PARENT_DIR
)
n_members_highECS = len(members_highECS)
print(f"  {n_members_highECS} / {len(MEMBER_IDS)} members loaded\n")

print("Loading v3.LR.lowECS.historical …")
ds_lowECS, members_lowECS = load_large_ensemble(
    ENSEMBLE_PREFIXES["lowECS"], MEMBER_IDS, VARIABLES, XLE_PARENT_DIR
)
n_members_lowECS = len(members_lowECS)
print(f"  {n_members_lowECS} / {len(MEMBER_IDS)} members loaded\n")

print("Summary")
print(f"  default    : {n_members_default} members — {members_default}")
print(f"  highECS    : {n_members_highECS} members — {members_highECS}")
print(f"  lowECS     : {n_members_lowECS} members — {members_lowECS}")

# ── Add time bounds variables and center time coordinates ─────────────────────

def add_time_bnds(ds):
    """
    Add a time_bnds variable to a dataset whose time coordinate represents
    the right bound of each monthly interval (i.e., first day of the following
    month).  The left bound is set to the first day of the current month at
    00:00:00, matching the E3SM convention.
    """
    times = ds["time"].values

    def left_bound(dt):
        year, month = dt.year, dt.month - 1
        if month == 0:
            month, year = 12, year - 1
        return type(dt)(year, month, 1, 0, 0, 0, 0, has_year_zero=dt.has_year_zero)

    left_bnds  = np.array([left_bound(t) for t in times])
    right_bnds = times

    time_bnds = xr.DataArray(
        np.array(list(zip(left_bnds, right_bnds))),
        dims=["time", "bnds"],
        coords={"time": ds["time"]},
    )
    ds = ds.assign({"time_bnds": time_bnds})
    ds["time"].attrs["bounds"] = "time_bnds"
    return ds

ds_default = xcdat.center_times(add_time_bnds(ds_default))
ds_highECS = xcdat.center_times(add_time_bnds(ds_highECS))
ds_lowECS  = xcdat.center_times(add_time_bnds(ds_lowECS))


'''
The function creates four subplots (CLDHGH, CLDMED, CLDLOW, TREFHT) for a chosen region
and overlays the ensemble members from  datasets using distinct colours.
''' 

"""Plot ensemble timeseries for three datasets (high, low, and a default reference).

Four sub‑plots (``CLDHGH``, ``CLDMED``, ``CLDLOW``, ``TREFHT``) are created for a
chosen region. All three datasets must be supplied; the default reference is
plotted in a dark‑gray colour.
"""




def _get_time_index(ds: xr.Dataset) -> pd.DatetimeIndex:
    """
    Convert the dataset’s ``time`` coordinate to a pandas ``DatetimeIndex``.

    Preferred method (the one you will use):
        time_idx = ds.indexes['time']
        x = time_idx.to_datetimeindex(unsafe=True)

    If the dataset does not expose ``indexes['time']`` the function falls back
    to a generic conversion that also handles ``cftime`` objects.
    """
    # 1️⃣ Try the explicit index route (the one you want)
    try:
        if "time" in ds.indexes:
            return ds.indexes["time"].to_datetimeindex(unsafe=True)
    except Exception:
        pass

    # 2️⃣ Generic handling for raw arrays
    first = ds.time.values.flat[0]
    if isinstance(first, (np.datetime64, pd.Timestamp)):
        return pd.to_datetime(ds.time.values)
    if isinstance(first, cftime.datetime):
        return pd.to_datetime(ds.time.values.tolist())

    # 3️⃣ Last‑ditch conversion (may raise an informative error)
    return pd.to_datetime(ds.time.values)


def plot_ensembles(
    ds_high: xr.Dataset,
    ds_low: xr.Dataset,
    ds_default: xr.Dataset,
    region_index: int = 0,
    variables: List[str] = None,
    figsize: Tuple[float, float] = (12, 16),
    high_color: str = "red",        # colour for high‑ECS
    low_color: str = "blue",         # colour for low‑ECS (distinguish via legend)
    default_color: str = "dimgray",   # colour for the default reference
    line_alpha: float = 0.3,
    line_width: float = 0.8,
    annual=True
) -> plt.Figure:
    """
    Create a 4‑row, 1‑column figure (CLDHGH → CLDMED → CLDLOW → TREFHT)
    overlaying the three ensemble datasets.

    Parameters
    ----------
    ds_high, ds_low, ds_default : xr.Dataset
        Must contain the four variables with dimensions (ensemble, time, rgn).
    region_index : int, default 0
        Index of the region to plot (0 ≈ global).
    variables : list of str, optional
        Plot order; defaults to ['CLDHGH', 'CLDMED', 'CLDLOW', 'TREFHT'].
    figsize : tuple, optional
        Figure size in inches.
    high_color, low_color, default_color : str, optional
        Named Matplotlib colours.
    line_alpha, line_width : float, optional
        Styling for individual ensemble members.

    Returns
    -------
    plt.Figure
        The finished Matplotlib figure.
    """
    # ------------------------------------------------------------------ #
    # 1️⃣ Determine which variables to plot
    # ------------------------------------------------------------------ #
    default_vars = ["CLDHGH", "CLDMED", "CLDLOW", "TREFHT"]
    vars_to_plot = variables if variables is not None else default_vars

    # ------------------------------------------------------------------ #
    # 2️⃣ Extract time axes (using the helper that respects your workflow)
    # ------------------------------------------------------------------ #
    time_high = _get_time_index(ds_high)
    time_low = _get_time_index(ds_low)
    time_def = _get_time_index(ds_default)

    # Combine all times so the x‑limits are identical across panels
    all_times = time_high.union(time_low).union(time_def)

    # ------------------------------------------------------------------ #
    # 3️⃣ Build the figure (4 rows × 1 column)
    # ------------------------------------------------------------------ #
    fig, axes = plt.subplots(
        nrows=4,
        ncols=1,
        figsize=figsize,
        sharex=False,            # we set limits manually below
        constrained_layout=True,
    )

    # ------------------------------------------------------------------ #
    # 4️⃣ Loop over the four variables and plot the three ensembles
    # ------------------------------------------------------------------ #
    for ax, var_name in zip(axes, vars_to_plot):
        # Verify variable exists in every dataset
        if var_name not in ds_high or var_name not in ds_low or var_name not in ds_default:
            raise KeyError(
                f"Variable '{var_name}' missing in one of the supplied datasets."
            )

        # Slice the requested region (shape: ensemble × time)
        high = ds_high[var_name].isel(rgn=region_index)
        low = ds_low[var_name].isel(rgn=region_index)
        default = ds_default[var_name].isel(rgn=region_index)

        if annual:
            high = high.groupby('time.year').mean('time')
            low = low.groupby('time.year').mean('time')
            default = default.groupby('time.year').mean('time')
            time_high = high.year
            time_low = low.year
            time_def =  default.year


        # ----- Plot individual ensemble members (thin, semi‑transparent) -----
        for i in range(high.sizes["ensemble"]):
            ax.plot(
                time_high,
                high.isel(ensemble=i).values,
                color=high_color,
                alpha=line_alpha,
                linewidth=line_width,
            )
        for i in range(low.sizes["ensemble"]):
            ax.plot(
                time_low,
                low.isel(ensemble=i).values,
                color=low_color,
                alpha=line_alpha,
                linewidth=line_width,
            )
        for i in range(default.sizes["ensemble"]):
            ax.plot(
                time_def,
                default.isel(ensemble=i).values,
                color=default_color,
                alpha=0.15,               # fainter raw members for the default set
                linewidth=line_width,
            )

        # ----- Plot ensemble means (thick lines) -----
        mean_high = high.mean(dim="ensemble")
        mean_low = low.mean(dim="ensemble")
        mean_def = default.mean(dim="ensemble")

        ax.plot(
            time_high,
            mean_high.values,
            color=high_color,
            linewidth=2,
            label="High ECS mean",
        )
        ax.plot(
            time_low,
            mean_low.values,
            color=low_color,
            linewidth=2,
            label="Low ECS mean",
        )
        ax.plot(
            time_def,
            mean_def.values,
            color=default_color,
            linewidth=2,
            linestyle="--",
            label="Default mean",
        )

        # ----- Axis cosmetics -----
        ax.set_ylabel(var_name)
        ax.grid(True, linestyle=":", alpha=0.5)
        ax.legend(loc="upper right")

        #ax.set_xlim(all_times[0], all_times[-1])

    # Bottom axis label & nice date formatting
    axes[-1].set_xlabel("Time")
    fig.autofmt_xdate()
    print('saving cloudfrac_timeseries.png')
    plt.savefig('cloudfrac_timeseries.png')
    #return fig


plot_ensembles(ds_highECS, ds_lowECS, ds_default, annual=True)


