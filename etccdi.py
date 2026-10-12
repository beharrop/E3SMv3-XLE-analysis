"""Compute ETCCDI climate indices from daily ERA5 or E3SM data.

ERA5 input is read one year at a time from annual files. E3SM input is read
from five-year files, with one output checkpoint per input chunk. E3SM uses
TREFHT directly for daily mean temperature and converts PRECT from m s-1 to
kg m-2 s-1 before computing precipitation indices.

Examples:
    python etccdi.py
    python etccdi.py --start_year 2015 --end_year 2020
    python etccdi.py --source e3sm --case v3.LR.historical_0051
    python etccdi.py --source e3sm --case v3.LR.historical_0051 --make_plots
    python etccdi.py --source e3sm --case v3.LR.historical_0051 --plots_only

Plots are not generated unless --make_plots or --plots_only is specified.
"""

import argparse
import time
from pathlib import Path

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_ROOT = Path("/global/cfs/cdirs/m3522/datalake/ERA5/postprocess")
E3SM_DATA_ROOT = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")
E3SM_DAILY_RELATIVE_PATH = Path("post/atm/180x360_aave/ts/daily/5yr")

ERA5_OUTPUT_ROOT = Path("/pscratch/sd/b/beharrop/ERA5/ETCCDI")
E3SM_OUTPUT_ROOT = Path("/pscratch/sd/b/beharrop/E3SMv3/XLE/ETCCDI")

START_YEAR = 1980
END_YEAR   = 2024
E3SM_START_YEAR = 1985
E3SM_END_YEAR   = 2014

RESULTS_DIR = ERA5_OUTPUT_ROOT / "results"
YEARS_DIR   = RESULTS_DIR / "by_year"
FIGS_DIR    = ERA5_OUTPUT_ROOT / "figures"
RESULTS_NC  = RESULTS_DIR / "ERA5_etccdi.nc"


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


def _file_year_bounds(path):
    """Return start/end years from a variable_YYYYMM_YYYYMM or _YYYY_YYYY name."""
    parts = path.stem.rsplit("_", 2)
    if (
        len(parts) != 3
        or len(parts[1]) not in {4, 6}
        or len(parts[2]) not in {4, 6}
    ):
        raise ValueError(f"Unexpected five-year E3SM filename: {path.name}")
    return int(parts[1][:4]), int(parts[2][:4])


def find_e3sm_chunks(input_dir, start_year, end_year):
    """Find E3SM five-year TREFHT files overlapping the requested years."""
    chunks = []
    for path in sorted(input_dir.glob("TREFHT_*.nc")):
        chunk_start, chunk_end = _file_year_bounds(path)
        if chunk_end >= start_year and chunk_start <= end_year:
            chunks.append((chunk_start, chunk_end))
    if not chunks:
        raise FileNotFoundError(
            f"No E3SM daily TREFHT files found for {start_year}-{end_year} in {input_dir}"
        )
    return chunks


def _load_e3sm_variable(path, variable):
    with xr.open_dataset(path) as source:
        if variable not in source:
            raise KeyError(f"Variable {variable!r} not found in {path}")
        data = source[variable]
        if "time_bnds" not in source:
            raise KeyError(f"Time bounds 'time_bnds' not found in {path}")
        time_bounds = source["time_bnds"]
        bound_dims  = [dim for dim in time_bounds.dims if dim != "time"]
        if "time" not in time_bounds.dims or len(bound_dims) != 1:
            raise ValueError(f"Unexpected time_bnds dimensions {time_bounds.dims} in {path}")
        time_attrs    = dict(data["time"].attrs)
        time_encoding = dict(data["time"].encoding)
        left_bound    = time_bounds.isel({bound_dims[0]: 0}, drop=True)
        data          = data.assign_coords(time=("time", left_bound.values))
        data["time"].attrs    = time_attrs
        data["time"].encoding = time_encoding
        rename = {
            old: new
            for old, new in (("latitude", "lat"), ("longitude", "lon"))
            if old in data.dims or old in data.coords
        }
        return data.rename(rename).load()


def load_e3sm_chunk(case, start_year, end_year):
    """Load one E3SM five-year chunk and normalize it for the xclim calls."""
    input_dir        = E3SM_DATA_ROOT / case / E3SM_DAILY_RELATIVE_PATH
    start_stamp      = f"{start_year}01"
    end_stamp        = f"{end_year}12"
    source_variables = ("TREFHT", "TREFHTMX", "TREFHTMN", "PRECT")
    arrays           = {}

    for variable in source_variables:
        path = input_dir / f"{variable}_{start_stamp}_{end_stamp}.nc"
        if not path.exists():
            raise FileNotFoundError(f"Missing E3SM input file: {path}")
        arrays[variable] = _load_e3sm_variable(path, variable)

    try:
        tas, tasmax, tasmin, prect = xr.align(
            *(arrays[name] for name in source_variables), join="exact", copy=False
        )
    except ValueError as exc:
        raise ValueError(
            f"E3SM variables do not share identical time and spatial coordinates "
            f"for {start_year}-{end_year}"
        ) from exc

    for variable, data in zip(source_variables[:3], (tas, tasmax, tasmin)):
        units = data.attrs.get("units", "").strip().lower()
        if units not in {"k", "kelvin"}:
            raise ValueError(f"Expected {variable} in kelvin, found units {units!r}")

    prect_units = prect.attrs.get("units", "").strip().lower().replace(" ", "")
    if prect_units not in {"ms-1", "ms^-1", "ms**-1", "m/s"}:
        raise ValueError(f"Expected PRECT in m s-1, found units {prect.attrs.get('units')!r}")

    ds = xr.Dataset({
        "tas": tas.rename("tas"),
        "tasmax": tasmax.rename("tasmax"),
        "tasmin": tasmin.rename("tasmin"),
        "pr": (prect * 1000.0).rename("pr"),
    })
    ds["tas"].attrs    = dict(ds["tas"].attrs)
    ds["tas"].attrs.update({"units": "K", 
                            "cell_methods": "time: mean",
                            "standard_name": "air_temperature", 
                            "long_name": "Daily Mean Surface Temperature"})
    ds["tasmax"].attrs = dict(ds["tasmax"].attrs)
    ds["tasmax"].attrs.update({"units": "K", 
                               "cell_methods": "time: maximum",
                               "standard_name": "air_temperature",
                               "long_name": "Daily Maximum Surface Temperature"})
    ds["tasmin"].attrs = dict(ds["tasmin"].attrs)
    ds["tasmin"].attrs.update({"units": "K", 
                               "cell_methods": "time: minimum",
                               "standard_name": "air_temperature",
                               "long_name": "Daily Minimum Surface Temperature"})
    ds["pr"].attrs     = dict(prect.attrs)
    ds["pr"].attrs.update({
        "units": "kg m-2 s-1",
        "standard_name": "precipitation_flux",
        "cell_methods": "time: mean",
        "long_name": "Precipitation",
    })
    return ds


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
    """Compute annual ETCCDI indices from one or more years of daily data."""
    from xclim import atmos

    tasmax = ds_daily["tasmax"]
    tasmin = ds_daily["tasmin"]
    tas    = ds_daily["tas"]
    pr     = ds_daily["pr"]  # normalized to kg m-2 s-1 by the source loader

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
    # pr is normalized to kg m-2 s-1 before reaching xclim.
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

    _ts(f"{year}: saving -> {out_f}")
    ds_etccdi.to_netcdf(out_f)
    del ds_etccdi


def combine_years(start_year, end_year, years_dir=YEARS_DIR, results_nc=RESULTS_NC):
    files = sorted(years_dir.glob("ERA5_etccdi_*.nc"))
    files = [f for f in files if start_year <= int(f.stem.split("_")[-1]) <= end_year]
    if not files:
        raise FileNotFoundError("No per-year files found — run without --plots_only first.")

    _ts(f"Combining {len(files)} yearly files -> {results_nc}")
    ds_all = xr.open_mfdataset([str(f) for f in files], combine="by_coords", decode_timedelta=False).load()
    ds_all.to_netcdf(results_nc)
    return ds_all


def process_e3sm_chunk(case, start_year, end_year, years_dir):
    out_file = years_dir / f"E3SM_etccdi_{start_year}_{end_year}.nc"
    if out_file.exists():
        _ts(f"E3SM {case} {start_year}-{end_year}: already computed, skipping")
        return

    _ts(f"E3SM {case} {start_year}-{end_year}: loading five-year chunk...")
    ds_daily = load_e3sm_chunk(case, start_year, end_year)
    _ts(f"E3SM {case} {start_year}-{end_year}: computing indices...")
    ds_etccdi = compute_etccdi_indices(ds_daily)
    ds_daily.close()
    ds_etccdi.attrs.update({"source": "E3SM", "case": case})

    _ts(f"E3SM {case} {start_year}-{end_year}: saving -> {out_file}")
    ds_etccdi.to_netcdf(out_file)
    ds_etccdi.close()


def combine_e3sm_chunks(start_year, end_year, years_dir, results_nc):
    files = []
    for path in sorted(years_dir.glob("E3SM_etccdi_*.nc")):
        chunk_start, chunk_end = _file_year_bounds(path)
        if chunk_end >= start_year and chunk_start <= end_year:
            files.append(path)
    if not files:
        raise FileNotFoundError("No E3SM chunk files found — run without --plots_only first.")

    _ts(f"Combining {len(files)} E3SM chunks -> {results_nc}")
    ds_all = xr.open_mfdataset(
        [str(path) for path in files], combine="by_coords", decode_timedelta=False
    ).load()
    year = ds_all["time"].dt.year
    ds_all = ds_all.sel(time=(year >= start_year) & (year <= end_year))
    ds_all.to_netcdf(results_nc)
    return ds_all


# ── Plotting ───────────────────────────────────────────────────────────────

def area_weighted_mean(da, lat):
    cos_w = np.cos(np.deg2rad(lat))
    return da.weighted(xr.DataArray(cos_w, dims="lat", coords={"lat": lat})).mean(dim=["lat", "lon"])


def make_plots(ds_etccdi, figs_dir=FIGS_DIR, label="ERA5", filename_prefix="ERA5"):
    """Generate summary plots for each ETCCDI index."""
    _ts("Generating plots...")
    figs_dir.mkdir(parents=True, exist_ok=True)
    lat = ds_etccdi["lat"].values

    for var in ds_etccdi.data_vars:
        da = ds_etccdi[var]
        if np.issubdtype(da.dtype, np.timedelta64):
            da = da / np.timedelta64(1, "D")

        # Area-weighted global mean time series (plain lat/lon mean over-weights
        # the poles on this regular-angle grid)
        fig, ax = plt.subplots(figsize=(10, 4))
        area_weighted_mean(da, lat).plot(ax=ax)
        ax.set_title(f"{label} — {var} area-weighted global mean time series")
        ax.set_xlabel("Year")
        ax.set_ylabel(ds_etccdi[var].attrs.get("units", ""))
        plt.tight_layout()
        fig.savefig(figs_dir / f"{filename_prefix}_{var}_timeseries.png", dpi=150)
        plt.close(fig)

        # Time-mean map
        fig, ax = plt.subplots(figsize=(12, 5))
        da.mean(dim="time").plot(ax=ax, cmap="viridis")
        ax.set_title(f"{label} — {var} time mean")
        plt.tight_layout()
        fig.savefig(figs_dir / f"{filename_prefix}_{var}_map.png", dpi=150)
        plt.close(fig)

    _ts("Plots saved.")


def main():
    parser = argparse.ArgumentParser(description="Compute ETCCDI indices from ERA5 or E3SM daily data")
    parser.add_argument("--source", choices=("era5", "e3sm"), default="era5",
                        help="Input data source (default: era5)")
    parser.add_argument("--case", help="E3SM case name, required when --source=e3sm")
    parser.add_argument("--start_year", type=int,
                        help="First year to process (default: source-specific)")
    parser.add_argument("--end_year", type=int,
                        help="Last year to process (default: source-specific)")
    parser.add_argument("--plots_only", action="store_true",
                        help="Skip computation and generate plots from existing results")
    parser.add_argument("--make_plots", action="store_true",
                        help="Generate plots after processing (off by default)")
    args = parser.parse_args()

    if args.source == "e3sm" and not args.case:
        parser.error("--case is required when --source=e3sm")
    if args.source == "era5" and args.case:
        parser.error("--case can only be used with --source=e3sm")
    if args.case and ("/" in args.case or "\\" in args.case):
        parser.error("--case must be a case name, not a path")

    default_start = E3SM_START_YEAR if args.source == "e3sm" else START_YEAR
    default_end   = E3SM_END_YEAR   if args.source == "e3sm" else END_YEAR
    start_year    = args.start_year if args.start_year is not None else default_start
    end_year      = args.end_year   if args.end_year   is not None else default_end
    if start_year > end_year:
        parser.error("--start_year must be less than or equal to --end_year")

    if args.source == "era5":
        output_root     = ERA5_OUTPUT_ROOT
        results_nc      = output_root / "results" / "ERA5_etccdi.nc"
        years_dir       = output_root / "results" / "by_year"
        figs_dir        = output_root / "figures"
        output_label    = "ERA5"
        filename_prefix = "ERA5"
    else:
        output_root     = E3SM_OUTPUT_ROOT / args.case
        results_nc      = output_root / "results" / "E3SM_etccdi.nc"
        years_dir       = output_root / "results" / "by_year"
        figs_dir        = output_root / "figures"
        output_label    = f"E3SM {args.case}"
        filename_prefix = f"E3SM_{args.case}"

    results_nc.parent.mkdir(parents=True, exist_ok=True)
    years_dir.mkdir(parents=True, exist_ok=True)

    if not args.plots_only:
        if args.source == "era5":
            for year in range(start_year, end_year + 1):
                process_year(year)
        else:
            input_dir = E3SM_DATA_ROOT / args.case / E3SM_DAILY_RELATIVE_PATH
            chunks = find_e3sm_chunks(input_dir, start_year, end_year)
            for chunk_start, chunk_end in chunks:
                process_e3sm_chunk(args.case, chunk_start, chunk_end, years_dir)

    if args.plots_only and results_nc.exists():
        _ts(f"Loading combined results from {results_nc}...")
        ds_etccdi = xr.open_dataset(results_nc, decode_timedelta=False)
    else:
        if args.source == "era5":
            ds_etccdi = combine_years(start_year, end_year, years_dir, results_nc)
        else:
            ds_etccdi = combine_e3sm_chunks(start_year, end_year, years_dir, results_nc)

    if args.make_plots or args.plots_only:
        make_plots(ds_etccdi, figs_dir, output_label, filename_prefix)
    _ts("All done.")


if __name__ == "__main__":
    main()
