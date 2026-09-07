""" ETCCDI climate extreme indices from ERA5 daily data.
Loads daily surface temperature (tasmax, tasmin) and precipitation (pr)
from annual NetCDF files, computes tas as mean of tasmax and tasmin,
then computes standard ETCCDI indices using xclim.

Processes one year at a time and checkpoints each year to its own file.
The original version built one dask graph over the full 45-year record
across 3 variables and only forced computation at to_netcdf() — several
indices (notably the consecutive dry/wet day counts) ended up needing
the whole record in memory at once, which OOM-killed the job around
~450GB RSS on a 503GB node. Every index here is a freq="YS" annual
index with no cross-year dependency, so per-year chunking is exact,
not an approximation — and it caps peak memory at ~3 files/year
(~4.5GB) regardless of record length, plus survives a crash without
losing already-computed years.

Input:
/global/cfs/cdirs/m3522/datalake/ERA5/postprocess/tasmax.e5.accum_daily/tasmax.e5.accum_daily.YYYY.nc
/global/cfs/cdirs/m3522/datalake/ERA5/postprocess/tasmin.e5.accum_daily/tasmin.e5.accum_daily.YYYY.nc
/global/cfs/cdirs/m3522/datalake/ERA5/postprocess/pr.e5.accum_daily_utc00/pr.e5.accum_daily.YYYY.nc

Output:
/pscratch/sd/m/mahf708/ERA5/ETCCDI/results/by_year/ERA5_etccdi_YYYY.nc  (per-year checkpoints)
/pscratch/sd/m/mahf708/ERA5/ETCCDI/results/ERA5_etccdi.nc              (combined)
/pscratch/sd/m/mahf708/ERA5/ETCCDI/figures/*.png

Usage:
python ct-test.py                              # process 1980-2024 and plot
python ct-test.py --start_year 2015 --end_year 2020
python ct-test.py --plots_only                 # skip computation, just plot
"""

import argparse
import time
from pathlib import Path

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_ROOT   = Path("/global/cfs/cdirs/m3522/datalake/ERA5/postprocess")
OUTPUT_ROOT = Path("/pscratch/sd/m/mahf708/ERA5/ETCCDI")
RESULTS_DIR = OUTPUT_ROOT / "results"
YEARS_DIR   = RESULTS_DIR / "by_year"
FIGS_DIR    = OUTPUT_ROOT / "figures"

START_YEAR = 1980
END_YEAR   = 2024

RESULTS_NC = RESULTS_DIR / "ERA5_etccdi.nc"


def _ts(msg):
    elapsed = time.time() - _ts.t0
    m, s = int(elapsed // 60), int(elapsed % 60)
    print(f"  [{m:02d}:{s:02d}] {msg}", flush=True)

_ts.t0 = time.time()


def load_one_year(year):
    """Load a single year of tasmax/tasmin/pr, in memory (no dask needed —
    one year of all three variables is only a few GB)."""
    tasmax_f = DATA_ROOT / "tasmax.e5.accum_daily" / f"tasmax.e5.accum_daily.{year}.nc"
    tasmin_f = DATA_ROOT / "tasmin.e5.accum_daily" / f"tasmin.e5.accum_daily.{year}.nc"
    pr_f     = DATA_ROOT / "pr.e5.accum_daily_utc00" / f"pr.e5.accum_daily.{year}.nc"

    ds_tasmax = xr.open_dataset(tasmax_f).rename({"latitude": "lat", "longitude": "lon"})
    ds_tasmin = xr.open_dataset(tasmin_f).rename({"latitude": "lat", "longitude": "lon"})
    ds_pr     = xr.open_dataset(pr_f).rename({"latitude": "lat", "longitude": "lon"})

    ds = xr.Dataset({
        "tasmax": ds_tasmax["tasmax"],
        "tasmin": ds_tasmin["tasmin"],
        "pr":     ds_pr["pr"],
    })
    ds["tas"] = (ds["tasmax"] + ds["tasmin"]) / 2.0

    ds["tasmax"].attrs = {
        "units": "K", "standard_name": "air_temperature",
        "cell_methods": "time: maximum", "long_name": "Daily Maximum Surface Temperature",
    }
    ds["tasmin"].attrs = {
        "units": "K", "standard_name": "air_temperature",
        "cell_methods": "time: minimum", "long_name": "Daily Minimum Surface Temperature",
    }
    ds["tas"].attrs = {
        "units": "K", "standard_name": "air_temperature",
        "cell_methods": "time: mean", "long_name": "Daily Mean Surface Temperature",
    }
    ds["pr"].attrs = {
        "units": "kg m-2 s-1", "standard_name": "precipitation_flux",
        "cell_methods": "time: mean", "long_name": "Precipitation",
    }

    ds_tasmax.close()
    ds_tasmin.close()
    ds_pr.close()
    return ds.load()


def growing_season_length_global(tas):
    """GSL, computed globally with xclim's default mid_date="07-01".

    NOTE: this is Northern-Hemisphere-biased. xclim's growing_season_length
    looks for the season start from Jan 1 and the season end in the first
    qualifying cold spell after mid_date; a naive mid_date="01-01" swap for
    the Southern Hemisphere degenerates (start/end search collapse) and
    returns 0 everywhere south of the equator — confirmed by testing, not
    a fix. A correct SH treatment needs the analysis year shifted 6 months
    (Jul-Jun), which requires two calendar years of data per "season year"
    and doesn't fit the per-year chunking here. Left as the same known
    limitation the original script had, not silently made worse.
    """
    from xclim import atmos

    return atmos.growing_season_length(tas=tas, freq="YS")


def compute_etccdi_indices(ds_daily):
    """Compute ETCCDI indices from one year of daily data using xclim."""
    from xclim import atmos

    tasmax = ds_daily["tasmax"]
    tasmin = ds_daily["tasmin"]
    tas    = ds_daily["tas"]
    pr     = ds_daily["pr"]  # already in kg m-2 s-1, no conversion needed

    results = {}

    # ── Temperature indices ───────────────────────────────────────────────
    results["TXx"] = atmos.tx_max(tasmax=tasmax, freq="YS")
    results["TNn"] = atmos.tn_min(tasmin=tasmin, freq="YS")
    results["TXn"] = atmos.tx_min(tasmax=tasmax, freq="YS")
    results["TNx"] = atmos.tn_max(tasmin=tasmin, freq="YS")
    results["DTR"] = atmos.daily_temperature_range(tasmax=tasmax, tasmin=tasmin, freq="YS")
    results["FD"]  = atmos.frost_days(tasmin=tasmin, freq="YS")
    results["SU"]  = atmos.tx_days_above(tasmax=tasmax, thresh="25 degC", freq="YS")
    results["ID"]  = atmos.ice_days(tasmax=tasmax, freq="YS")
    results["TR"]  = atmos.tropical_nights(tasmin=tasmin, freq="YS")
    results["GSL"] = growing_season_length_global(tas)

    # ── Precipitation indices ─────────────────────────────────────────────
    # pr is already in kg m-2 s-1, pass directly to xclim
    results["RX1day"]  = atmos.max_1day_precipitation_amount(pr=pr, freq="YS")
    results["RX5day"]  = atmos.max_n_day_precipitation_amount(pr=pr, window=5, freq="YS")
    results["SDII"]    = atmos.daily_pr_intensity(pr=pr, thresh="1 mm/day", freq="YS")
    results["CDD"]     = atmos.maximum_consecutive_dry_days(pr=pr, thresh="1 mm/day", freq="YS")
    results["CWD"]     = atmos.maximum_consecutive_wet_days(pr=pr, thresh="1 mm/day", freq="YS")
    results["R10mm"]   = atmos.wetdays(pr=pr, thresh="10 mm/day", freq="YS")
    results["R20mm"]   = atmos.wetdays(pr=pr, thresh="20 mm/day", freq="YS")
    results["PRCPTOT"] = atmos.wet_precip_accumulation(pr=pr, thresh="1 mm/day", freq="YS")

    return xr.Dataset(results)


def process_year(year):
    out_f = YEARS_DIR / f"ERA5_etccdi_{year}.nc"
    if out_f.exists():
        _ts(f"{year}: already computed, skipping")
        return

    _ts(f"{year}: loading …")
    ds_daily = load_one_year(year)

    _ts(f"{year}: computing indices …")
    ds_etccdi = compute_etccdi_indices(ds_daily)
    ds_daily.close()
    del ds_daily

    _ts(f"{year}: saving → {out_f}")
    ds_etccdi.to_netcdf(out_f)
    del ds_etccdi


def combine_years(start_year, end_year):
    files = sorted(YEARS_DIR.glob("ERA5_etccdi_*.nc"))
    files = [f for f in files if start_year <= int(f.stem.split("_")[-1]) <= end_year]
    if not files:
        raise FileNotFoundError("No per-year files found — run without --plots_only first.")

    _ts(f"Combining {len(files)} yearly files → {RESULTS_NC}")
    ds_all = xr.open_mfdataset([str(f) for f in files], combine="by_coords", decode_timedelta=False).load()
    ds_all.to_netcdf(RESULTS_NC)
    return ds_all


# ── Plotting ───────────────────────────────────────────────────────────────

def area_weighted_mean(da, lat):
    cos_w = np.cos(np.deg2rad(lat))
    return da.weighted(xr.DataArray(cos_w, dims="lat", coords={"lat": lat})).mean(dim=["lat", "lon"])


def make_plots(ds_etccdi):
    """Generate summary plots for each ETCCDI index."""
    _ts("Generating plots...")
    lat = ds_etccdi["lat"].values

    for var in ds_etccdi.data_vars:
        da = ds_etccdi[var]
        if np.issubdtype(da.dtype, np.timedelta64):
            da = da / np.timedelta64(1, "D")

        # Area-weighted global mean time series (plain lat/lon mean over-weights
        # the poles on this regular-angle grid)
        fig, ax = plt.subplots(figsize=(10, 4))
        area_weighted_mean(da, lat).plot(ax=ax)
        ax.set_title(f"ERA5 — {var} area-weighted global mean time series")
        ax.set_xlabel("Year")
        ax.set_ylabel(ds_etccdi[var].attrs.get("units", ""))
        plt.tight_layout()
        fig.savefig(FIGS_DIR / f"ERA5_{var}_timeseries.png", dpi=150)
        plt.close(fig)

        # Time-mean map
        fig, ax = plt.subplots(figsize=(12, 5))
        da.mean(dim="time").plot(ax=ax, cmap="viridis")
        ax.set_title(f"ERA5 — {var} time mean")
        plt.tight_layout()
        fig.savefig(FIGS_DIR / f"ERA5_{var}_map.png", dpi=150)
        plt.close(fig)

    _ts("Plots saved.")


def main():
    parser = argparse.ArgumentParser(description="ETCCDI indices from ERA5 daily data")
    parser.add_argument("--start_year", type=int, default=START_YEAR)
    parser.add_argument("--end_year", type=int, default=END_YEAR)
    parser.add_argument("--plots_only", action="store_true",
                        help="Skip computation, just regenerate plots")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    YEARS_DIR.mkdir(parents=True, exist_ok=True)
    FIGS_DIR.mkdir(parents=True, exist_ok=True)

    if not args.plots_only:
        for year in range(args.start_year, args.end_year + 1):
            process_year(year)

    if args.plots_only and RESULTS_NC.exists():
        _ts(f"Loading combined results from {RESULTS_NC} …")
        ds_etccdi = xr.open_dataset(RESULTS_NC, decode_timedelta=False)
    else:
        ds_etccdi = combine_years(args.start_year, args.end_year)

    make_plots(ds_etccdi)
    _ts("All done.")


if __name__ == "__main__":
    main()
