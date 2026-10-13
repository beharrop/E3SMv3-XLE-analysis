#!/usr/bin/env python3
"""
Make 6-panel spatial maps of SWCF and LWCF from E3SM v3.XLE large ensembles
compared against CERES-EBAF observations.

For each variable, this script writes one PNG file:

    plots/SWCF_model_obs_6panel_map_1985_2014.png
    plots/LWCF_model_obs_6panel_map_1985_2014.png

Layout:

    Top row, left-to-right:
        Default, Low-ECS, High-ECS

    Bottom row, left-to-right:
        Default - Obs, Low-ECS - Obs, High-ECS - Obs

Model fields:
    E3SM v3.XLE ensemble means, time averaged over 1985-2014.

Observations:
    CERES-EBAF TOA v4.1 annual climatology, 2001-2018.
    SWCF obs uses toa_cre_sw_mon.
    LWCF obs uses toa_cre_lw_mon.

Speed:
    The script only opens 5-year timeseries files whose filename-implied year
    range overlaps 1985-2014. Each member is time-averaged before concatenating
    across ensemble members.
"""

import re
from pathlib import Path
from typing import List, Tuple
from functools import partial

import numpy as np
import xarray as xr
from scipy.interpolate import griddata

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.util import add_cyclic_point


# ── Configuration ─────────────────────────────────────────────────────────────

XLE_PARENT_DIR = Path("/global/cfs/cdirs/e3smdata/simulations/v3.XLE")

# Used for native-grid lat/lon if the timeseries files do not carry them.
GRID_FILE = Path("native_area_lat_lon.nc")

OBS_FILE = Path(
    "/global/cfs/cdirs/e3sm/e3sm_diags/obs_for_e3sm_diags/climatology/"
    "ceres_ebaf_toa_v4.1/ceres_ebaf_toa_v4.1_ANN_200101_201812_climo.nc"
)

OBS_VAR_MAP = {
    "SWCF": "toa_cre_sw_mon",
    "LWCF": "toa_cre_lw_mon",
}

MEMBER_IDS = [
    "0051", "0091", "0101",
    "0111", "0121", "0131", "0141", "0151",
    "0161", "0171", "0181", "0191", "0201", "0211", "0221", "0231",
    "0241", "0251", "0261", "0271", "0281", "0291", "0301", "0311", "0321",
]

ENSEMBLE_PREFIXES = {
    "default": "v3.LR.historical",
    "highECS": "v3.LR.highECS.historical",
    "lowECS":  "v3.LR.lowECS.historical",
}

VARIABLES = ["SWCF", "LWCF"]

START_YEAR = 1985
END_YEAR = 2014

# Leave as None for all months.
# Example: MONTHS = [6, 7, 8] for JJA.
MONTHS = None

OUT_DIR = Path("plots")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── File filtering utilities ──────────────────────────────────────────────────

def make_white_centered_diff_cmap(
    diff_abs_max: float,
    white_half_width: float = 10.0,
    base_cmap: str = "RdBu_r",
    ncolors: int = 256,
):
    """
    Create a diverging colormap with a white band from
    -white_half_width to +white_half_width.

    Example:
        white_half_width=10 gives white from -10 to +10.
    """

    if not np.isfinite(diff_abs_max) or diff_abs_max <= 0:
        diff_abs_max = white_half_width

    # Make sure range is wider than the white band.
    diff_abs_max = max(diff_abs_max, white_half_width * 1.01)

    base = plt.get_cmap(base_cmap, ncolors)
    colors = base(np.linspace(0, 1, ncolors))

    vals = np.linspace(-diff_abs_max, diff_abs_max, ncolors)
    white_mask = (vals >= -white_half_width) & (vals <= white_half_width)

    colors[white_mask, :] = np.array([1.0, 1.0, 1.0, 1.0])

    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.Normalize(vmin=-diff_abs_max, vmax=diff_abs_max)

    return cmap, norm

def _extract_years_from_filename(path: Path) -> tuple[int | None, int | None]:
    """
    Extract start/end years from an E3SM timeseries filename.
    """

    name = path.name

    # Prefer YYYYMM-style dates.
    yyyymm = re.findall(r"((?:18|19|20|21)\d{2})(0[1-9]|1[0-2])", name)
    if len(yyyymm) >= 2:
        start_year = int(yyyymm[-2][0])
        end_year = int(yyyymm[-1][0])
        return start_year, end_year

    # Fall back to YYYY-style dates.
    years = re.findall(r"(?<!\d)((?:18|19|20|21)\d{2})(?!\d)", name)
    if len(years) >= 2:
        return int(years[-2]), int(years[-1])
    if len(years) == 1:
        return int(years[0]), int(years[0])

    return None, None


def _filter_files_by_year(
    files: list[Path],
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[Path]:
    """
    Keep only files whose filename-implied year range overlaps the requested
    year window.
    """

    if start_year is None and end_year is None:
        return files

    keep = []

    for f in files:
        file_start, file_end = _extract_years_from_filename(f)

        # If parsing fails, keep the file rather than accidentally dropping data.
        if file_start is None or file_end is None:
            keep.append(f)
            continue

        if start_year is not None and file_end < start_year:
            continue

        if end_year is not None and file_start > end_year:
            continue

        keep.append(f)

    return keep


def _subset_time_preprocess(
    ds: xr.Dataset,
    start_year: int | None = None,
    end_year: int | None = None,
    months: list[int] | None = None,
) -> xr.Dataset:
    """
    Preprocess each file during open_mfdataset, immediately dropping unwanted
    time slices from boundary files.
    """

    if "time" not in ds:
        return ds

    mask = xr.ones_like(ds["time"].dt.year, dtype=bool)

    if start_year is not None:
        mask = mask & (ds["time"].dt.year >= start_year)

    if end_year is not None:
        mask = mask & (ds["time"].dt.year <= end_year)

    if months is not None:
        mask = mask & ds["time"].dt.month.isin(months)

    return ds.sel(time=mask)


# ── Coordinate utilities ──────────────────────────────────────────────────────

def _candidate_lon_names():
    return ["lon", "longitude", "LON", "LONGITUDE"]


def _candidate_lat_names():
    return ["lat", "latitude", "LAT", "LATITUDE"]


def _find_var_or_coord(ds: xr.Dataset | xr.DataArray, names: list[str]):
    """
    Find a variable or coordinate in a Dataset/DataArray from possible names.
    """

    for name in names:
        if name in ds.coords:
            return name, ds.coords[name]
        if hasattr(ds, "variables") and name in ds.variables:
            return name, ds[name]

    return None, None


def attach_lat_lon_from_grid_file(
    ds: xr.Dataset,
    grid_file: Path = GRID_FILE,
) -> xr.Dataset:
    """
    Attach lat/lon coordinates from GRID_FILE if ds does not already have them.

    This is useful for E3SM native-grid timeseries files where fields have an
    ncol dimension but lat/lon live in a separate file.
    """

    _, lon_existing = _find_var_or_coord(ds, _candidate_lon_names())
    _, lat_existing = _find_var_or_coord(ds, _candidate_lat_names())

    if lon_existing is not None and lat_existing is not None:
        return ds

    if not grid_file.exists():
        print(
            f"  [WARNING] Grid file {grid_file} not found and dataset lacks "
            "lat/lon coordinates."
        )
        return ds

    grid_ds = xr.open_dataset(grid_file)

    _, lon = _find_var_or_coord(grid_ds, _candidate_lon_names())
    _, lat = _find_var_or_coord(grid_ds, _candidate_lat_names())

    if lon is None or lat is None:
        print(
            f"  [WARNING] Could not find lat/lon in grid file {grid_file}. "
            f"Available variables: {list(grid_ds.variables)}"
        )
        grid_ds.close()
        return ds

    lon = lon.squeeze(drop=True)
    lat = lat.squeeze(drop=True)

    for coord_da, coord_label in [(lon, "lon"), (lat, "lat")]:
        for dim in coord_da.dims:
            if dim not in ds.dims:
                print(
                    f"  [WARNING] {coord_label} dimension {coord_da.dims} from "
                    f"{grid_file} does not match dataset dims {ds.dims}; "
                    "not attaching grid-file lat/lon."
                )
                grid_ds.close()
                return ds

            if coord_da.sizes[dim] != ds.sizes[dim]:
                print(
                    f"  [WARNING] {coord_label} size mismatch along {dim}: "
                    f"grid file has {coord_da.sizes[dim]}, dataset has {ds.sizes[dim]}; "
                    "not attaching grid-file lat/lon."
                )
                grid_ds.close()
                return ds

    ds = ds.assign_coords({"lon": lon.load(), "lat": lat.load()})
    grid_ds.close()

    print(f"  Attached lat/lon from {grid_file}")

    return ds


def _get_lat_lon_for_map(da: xr.DataArray, ds: xr.Dataset | None = None):
    """
    Find lat/lon coordinates for regular lat/lon or native unstructured grids.
    """

    search_objs = [da]
    if ds is not None:
        search_objs.append(ds)

    lon = None
    lat = None

    for obj in search_objs:
        _, candidate = _find_var_or_coord(obj, _candidate_lon_names())
        if candidate is not None:
            lon = candidate
            break

    for obj in search_objs:
        _, candidate = _find_var_or_coord(obj, _candidate_lat_names())
        if candidate is not None:
            lat = candidate
            break

    if lon is None or lat is None:
        raise ValueError(
            "Could not find latitude/longitude coordinates. "
            "Expected names like lat/lon or latitude/longitude. "
            "If using native-grid data, make sure native_area_lat_lon.nc exists "
            "and contains lat/lon."
        )

    return lon, lat


# ── Data loading ──────────────────────────────────────────────────────────────

def load_large_ensemble(
    prefix: str,
    member_ids: List[str],
    variables: List[str],
    parent_dir: Path,
    start_year: int | None = None,
    end_year: int | None = None,
    months: List[int] | None = None,
    average_time: bool = True,
) -> Tuple[xr.Dataset | None, List[str]]:
    """
    Load monthly native-grid timeseries data for one E3SM large ensemble.

    For each member, this function looks under:

        <parent_dir>/<prefix>_<member_id>/post/atm/native/ts/monthly/5yr/

    It only opens files overlapping start_year/end_year, then optionally
    time-averages each member before concatenating across ensemble.

    With average_time=True, output dimensions are typically:

        ensemble, ncol

    or:

        ensemble, lat, lon
    """

    member_datasets = []
    loaded_members = []

    for member_id in member_ids:
        ts_dir = (
            parent_dir
            / f"{prefix}_{member_id}"
            / "post" / "atm" / "native" / "ts" / "monthly" / "5yr"
        )

        print(f"\nChecking {ts_dir}")

        var_data_arrays = {}
        coord_vars = {}
        all_vars_present = True

        for var in variables:
            files_all = sorted(ts_dir.glob(f"{var}_*.nc"))
            files = _filter_files_by_year(
                files_all,
                start_year=start_year,
                end_year=end_year,
            )

            if not files:
                print(
                    f"  [MISSING] {prefix}_{member_id}: no files for {var} "
                    f"overlapping {start_year}-{end_year}"
                )
                all_vars_present = False
                break

            print(
                f"  {var}: using {len(files)} / {len(files_all)} files "
                f"for years {start_year}-{end_year}"
            )

            preprocess = partial(
                _subset_time_preprocess,
                start_year=start_year,
                end_year=end_year,
                months=months,
            )

            ds_var = xr.open_mfdataset(
                files,
                combine="nested",
                concat_dim="time",
                data_vars="minimal",
                coords="minimal",
                compat="override",
                parallel=False,
                preprocess=preprocess,
                chunks={"time": -1},
            )

            if var not in ds_var:
                print(f"  [MISSING] {prefix}_{member_id}: variable {var} not in files")
                all_vars_present = False
                ds_var.close()
                break

            da = ds_var[var]

            if "time" in da.dims:
                if da.sizes["time"] == 0:
                    print(
                        f"  [SKIP] {prefix}_{member_id}: no {var} data after "
                        f"time subset {start_year}-{end_year}"
                    )
                    all_vars_present = False
                    ds_var.close()
                    break

                if average_time:
                    print(f"  {var}: averaging over {da.sizes['time']} monthly values")
                    da = da.mean(dim="time", skipna=True).load()

            var_data_arrays[var] = da

            # Preserve lat/lon-like variables if present in timeseries files.
            for coord_name in _candidate_lat_names() + _candidate_lon_names():
                if coord_name in ds_var.variables and coord_name not in coord_vars:
                    coord = ds_var[coord_name]
                    if "time" in coord.dims:
                        coord = coord.isel(time=0, drop=True)
                    coord_vars[coord_name] = coord.load()

            ds_var.close()

        if not all_vars_present:
            print(f"  [SKIP] {prefix}_{member_id}: one or more variables absent")
            continue

        member_ds = xr.Dataset(var_data_arrays)

        for coord_name, coord_da in coord_vars.items():
            if coord_name not in member_ds.coords:
                member_ds = member_ds.assign_coords({coord_name: coord_da})

        member_ds = attach_lat_lon_from_grid_file(member_ds, GRID_FILE)

        member_datasets.append(member_ds)
        loaded_members.append(member_id)

        print(f"  [LOAD] {prefix}_{member_id}")

    if not member_datasets:
        print(f"  [WARNING] No members loaded for {prefix}")
        return None, []

    ensemble_coord = xr.DataArray(
        np.array(loaded_members, dtype=str),
        dims="ensemble",
        name="ensemble",
    )

    ds_ensemble = xr.concat(
        member_datasets,
        dim=ensemble_coord,
        data_vars="all",
        coords="minimal",
        compat="override",
        join="override",
    )

    ds_ensemble = attach_lat_lon_from_grid_file(ds_ensemble, GRID_FILE)

    return ds_ensemble, loaded_members


# ── Observation and regridding utilities ──────────────────────────────────────

def load_obs_cre(
    var_name: str,
    obs_file: Path = OBS_FILE,
    obs_var_map: dict = OBS_VAR_MAP,
) -> xr.DataArray:
    """
    Load CERES-EBAF observed CRE field corresponding to model SWCF or LWCF.

    Mapping:
        SWCF -> toa_cre_sw_mon
        LWCF -> toa_cre_lw_mon

    Returns
    -------
    xr.DataArray
        Observed CRE on regular lat/lon grid with dims lat, lon.
    """

    if var_name not in obs_var_map:
        raise KeyError(
            f"No obs variable mapping defined for {var_name}. "
            f"Available mappings: {obs_var_map}"
        )

    obs_var = obs_var_map[var_name]

    obs_ds = xr.open_dataset(obs_file)

    if obs_var not in obs_ds:
        raise KeyError(
            f"Observed variable {obs_var} not found in {obs_file}. "
            f"Available variables: {list(obs_ds.data_vars)}"
        )

    obs = obs_ds[obs_var]

    if "time" in obs.dims:
        obs = obs.isel(time=0, drop=True)

    # Mask large fill values if any remain.
    obs = obs.where(np.isfinite(obs))
    obs = obs.where(np.abs(obs) < 1.0e19)

    # Use 0..360 longitudes and sort.
    obs = obs.assign_coords(lon=obs["lon"] % 360).sortby("lon")

    obs.attrs["model_equivalent_var"] = var_name
    obs.attrs["obs_var"] = obs_var

    obs = obs.load()
    obs_ds.close()

    return obs


def ensemble_mean_map(ds: xr.Dataset, var_name: str, coeff: float = 1.0) -> xr.DataArray:
    """
    Compute ensemble-mean map.

    Time should already have been averaged during loading. If a time dimension
    remains, this function averages over it too.
    """

    if var_name not in ds:
        raise KeyError(
            f"Variable '{var_name}' not found. Available variables: {list(ds.data_vars)}"
        )

    da = ds[var_name] * coeff

    mean_dims = []

    if "ensemble" in da.dims:
        mean_dims.append("ensemble")

    if "time" in da.dims:
        mean_dims.append("time")

    if mean_dims:
        da = da.mean(dim=mean_dims, skipna=True)

    return da.squeeze(drop=True)


def _source_lon_lat_data_for_regrid(
    da: xr.DataArray,
    ds_for_coords: xr.Dataset | None = None,
):
    """
    Return flattened lon, lat, data arrays for either regular lat/lon or
    native/unstructured model data.
    """

    lon, lat = _get_lat_lon_for_map(da, ds=ds_for_coords)

    data_vals = np.asarray(da).squeeze()

    # Regular lat/lon with 1D lat/lon.
    if (
        "lat" in da.dims
        and "lon" in da.dims
        and np.asarray(lat).ndim == 1
        and np.asarray(lon).ndim == 1
    ):
        lon2d, lat2d = np.meshgrid(
            np.asarray(lon).squeeze() % 360,
            np.asarray(lat).squeeze(),
        )
        lon_vals = lon2d.ravel()
        lat_vals = lat2d.ravel()
        data_vals = data_vals.ravel()

    # Native/unstructured.
    else:
        lon_vals = np.asarray(lon).squeeze().ravel() % 360
        lat_vals = np.asarray(lat).squeeze().ravel()
        data_vals = data_vals.ravel()

    valid = (
        np.isfinite(lon_vals)
        & np.isfinite(lat_vals)
        & np.isfinite(data_vals)
    )

    return lon_vals[valid], lat_vals[valid], data_vals[valid]


def regrid_model_to_obs_grid(
    model_da: xr.DataArray,
    obs_da: xr.DataArray,
    ds_for_coords: xr.Dataset | None = None,
    method: str = "linear",
) -> xr.DataArray:
    """
    Regrid model field to the CERES obs lat/lon grid using scipy.griddata.

    This works for both native/unstructured and regular lat/lon model fields.

    To avoid a seam/white stripe near 0/360, longitudes are duplicated
    cyclically at lon-360 and lon+360 before interpolation.
    """

    src_lon, src_lat, src_data = _source_lon_lat_data_for_regrid(
        model_da,
        ds_for_coords=ds_for_coords,
    )

    obs_lon = np.asarray(obs_da["lon"]).squeeze() % 360
    obs_lat = np.asarray(obs_da["lat"]).squeeze()

    target_lon2d, target_lat2d = np.meshgrid(obs_lon, obs_lat)

    # Add cyclic longitude copies for smoother interpolation across 0/360.
    src_lon_cyc = np.concatenate(
        [
            src_lon - 360.0,
            src_lon,
            src_lon + 360.0,
        ]
    )
    src_lat_cyc = np.concatenate(
        [
            src_lat,
            src_lat,
            src_lat,
        ]
    )
    src_data_cyc = np.concatenate(
        [
            src_data,
            src_data,
            src_data,
        ]
    )

    points = np.column_stack([src_lon_cyc, src_lat_cyc])

    print(
        f"  Regridding to obs grid: source points={len(src_data)}, "
        f"target shape={target_lon2d.shape}"
    )

    regridded = griddata(
        points,
        src_data_cyc,
        (target_lon2d, target_lat2d),
        method=method,
    )

    # Fill any remaining holes with nearest-neighbor interpolation.
    if np.any(~np.isfinite(regridded)):
        nearest = griddata(
            points,
            src_data_cyc,
            (target_lon2d, target_lat2d),
            method="nearest",
        )
        regridded = np.where(np.isfinite(regridded), regridded, nearest)

    out = xr.DataArray(
        regridded,
        dims=("lat", "lon"),
        coords={
            "lat": obs_da["lat"],
            "lon": obs_da["lon"],
        },
        attrs=model_da.attrs,
        name=model_da.name,
    )

    out.attrs["regridded_to"] = "CERES-EBAF obs grid"
    out.attrs["regrid_method"] = method

    return out


# ── Plotting utilities ────────────────────────────────────────────────────────

def _setup_map_axis(ax):
    """
    Add common map features to one Cartopy GeoAxes.
    """

    ax.set_global()
    ax.coastlines(linewidth=0.7)
    ax.add_feature(cfeature.BORDERS, edgecolor="black", linewidth=0.4)

    gl = ax.gridlines(
        draw_labels=True,
        dms=True,
        x_inline=False,
        y_inline=False,
        linewidth=0.4,
        color="gray",
        alpha=0.5,
    )

    gl.top_labels = False
    gl.right_labels = False


def _plot_regular_latlon_panel(
    ax,
    da: xr.DataArray,
    cmap: str = "viridis",
    norm=None,
    vmin=None,
    vmax=None,
    levels=None,
    title: str | None = None,
):
    """
    Plot one regular lat/lon panel using pcolormesh.

    pcolormesh is more robust than contourf for regridded data because contourf
    can create white polygon artifacts around small masked/NaN regions.
    """

    data_crs = ccrs.PlateCarree()

    _setup_map_axis(ax)

    if title is not None:
        ax.set_title(title, fontsize=14, pad=12)

    da_plot = da.assign_coords(lon=da["lon"] % 360).sortby("lon")

    data_cyc, lon_cyc = add_cyclic_point(
        da_plot.values,
        coord=da_plot["lon"].values,
    )

    lon2d, lat2d = np.meshgrid(
        lon_cyc,
        da_plot["lat"].values,
    )

    im = ax.pcolormesh(
        lon2d,
        lat2d,
        data_cyc,
        transform=data_crs,
        cmap=cmap,
        norm=norm,
        vmin=vmin,
        vmax=vmax,
        shading="auto",
    )

    return im


def _safe_float(x) -> float:
    """
    Convert scalar DataArray-like objects to float.
    """

    return float(np.asarray(x).squeeze())


def _coslat_weighted_mean(da: xr.DataArray) -> float:
    """
    Area-weighted mean for regular lat/lon using cosine latitude weights.
    """

    if "lat" in da.dims and "lon" in da.dims:
        weights = np.cos(np.deg2rad(da["lat"]))
        mean_val = da.weighted(weights).mean(dim=["lat", "lon"], skipna=True)
        return float(np.asarray(mean_val).squeeze())

    return float(np.asarray(da.mean(skipna=True)).squeeze())


def _annotate_panel_mean(
    ax,
    mean_value: float,
    units: str | None = None,
    label: str = "mean",
):
    """
    Add a mean-value annotation just above the top-right of an axes.
    """

    text = f"{label} = {mean_value:0.2f}"
    if units:
        text += f" {units}"

    ax.text(
        0.98,
        1.035,
        text,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=10,
        bbox=dict(
            facecolor="white",
            edgecolor="none",
            alpha=0.78,
            boxstyle="round,pad=0.25",
        ),
        zorder=10,
    )


def plot_model_obs_6panel_map(
    ds_default: xr.Dataset,
    ds_low: xr.Dataset,
    ds_high: xr.Dataset,
    var_name: str,
    out_file: str | Path,
    obs_file: Path = OBS_FILE,
    coeff: float = 1.0,
    units: str | None = None,
    cmap: str = "viridis",
    cmap_diff: str = "RdBu_r",
    field_levels=None,
    diff_levels=None,
    figsize: Tuple[float, float] = (17.0, 8.2),
    dpi: int = 300,
):
    """
    Make a 6-panel model-vs-obs CRE map.

    Layout
    ------
    Top row:
        Default, Low-ECS, High-ECS

    Bottom row:
        Default - Obs, Low-ECS - Obs, High-ECS - Obs
    """

    out_file = Path(out_file)

    # ------------------------------------------------------------------
    # Model ensemble-mean maps on native grid
    # ------------------------------------------------------------------
    default_native = ensemble_mean_map(ds_default, var_name, coeff=coeff)
    low_native = ensemble_mean_map(ds_low, var_name, coeff=coeff)
    high_native = ensemble_mean_map(ds_high, var_name, coeff=coeff)

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------
    obs = load_obs_cre(var_name, obs_file=obs_file)

    if units is None:
        units = ds_default[var_name].attrs.get("units", obs.attrs.get("units", None))

    # ------------------------------------------------------------------
    # Regrid model maps to obs grid before differencing
    # ------------------------------------------------------------------
    print(f"\nRegridding {var_name} model maps to CERES obs grid")

    default_map = regrid_model_to_obs_grid(
        default_native,
        obs,
        ds_for_coords=ds_default,
        method="linear",
    )

    low_map = regrid_model_to_obs_grid(
        low_native,
        obs,
        ds_for_coords=ds_low,
        method="linear",
    )

    high_map = regrid_model_to_obs_grid(
        high_native,
        obs,
        ds_for_coords=ds_high,
        method="linear",
    )

    default_diff = default_map - obs
    low_diff = low_map - obs
    high_diff = high_map - obs

    # ------------------------------------------------------------------
    # Color limits
    # ------------------------------------------------------------------
    # Top row: model fields only.
    field_min = np.nanmin(
        [
            _safe_float(default_map.min(skipna=True)),
            _safe_float(low_map.min(skipna=True)),
            _safe_float(high_map.min(skipna=True)),
        ]
    )

    field_max = np.nanmax(
        [
            _safe_float(default_map.max(skipna=True)),
            _safe_float(low_map.max(skipna=True)),
            _safe_float(high_map.max(skipna=True)),
        ]
    )

    # Bottom row: model minus obs.
    diff_abs_max = np.nanmax(
        [
            _safe_float(np.abs(default_diff).max(skipna=True)),
            _safe_float(np.abs(low_diff).max(skipna=True)),
            _safe_float(np.abs(high_diff).max(skipna=True)),
        ]
    )

    if not np.isfinite(diff_abs_max) or diff_abs_max == 0.0:
        diff_abs_max = 1.0e-12

    diff_cmap_white, diff_norm = make_white_centered_diff_cmap(
        diff_abs_max=diff_abs_max,
        white_half_width=10.0,
        base_cmap=cmap_diff,
    )

    # ------------------------------------------------------------------
    # Mean labels
    # ------------------------------------------------------------------
    default_mean = _coslat_weighted_mean(default_map)
    low_mean = _coslat_weighted_mean(low_map)
    high_mean = _coslat_weighted_mean(high_map)
    obs_mean = _coslat_weighted_mean(obs)

    default_diff_mean = _coslat_weighted_mean(default_diff)
    low_diff_mean = _coslat_weighted_mean(low_diff)
    high_diff_mean = _coslat_weighted_mean(high_diff)

    print(f"\n{var_name} diagnostics")
    print(f"  Obs mean            : {obs_mean:0.3f}")
    print(f"  Default mean        : {default_mean:0.3f}")
    print(f"  Low ECS mean        : {low_mean:0.3f}")
    print(f"  High ECS mean       : {high_mean:0.3f}")
    print(f"  Default - Obs mean  : {default_diff_mean:0.3f}")
    print(f"  Low ECS - Obs mean  : {low_diff_mean:0.3f}")
    print(f"  High ECS - Obs mean : {high_diff_mean:0.3f}")

    # ------------------------------------------------------------------
    # Figure
    # ------------------------------------------------------------------
    map_proj = ccrs.Robinson(central_longitude=180)

    fig, ax = plt.subplots(
        nrows=2,
        ncols=3,
        figsize=figsize,
        subplot_kw={"projection": map_proj},
        constrained_layout=False,
    )

    fig.subplots_adjust(
    left=0.02,
    right=0.98,
    bottom=0.12,
    top=0.88,
    wspace=0.02,
    hspace=0.6,
)
    for a in ax.flat:
        a.set_anchor("C")

    # ------------------------------------------------------------------
    # Top row: model fields
    # ------------------------------------------------------------------
    im_default = _plot_regular_latlon_panel(
        ax[0, 0],
        default_map,
        cmap=cmap,
        vmin=field_min,
        vmax=field_max,
        levels=field_levels,
        title="Default",
    )

    im_low = _plot_regular_latlon_panel(
        ax[0, 1],
        low_map,
        cmap=cmap,
        vmin=field_min,
        vmax=field_max,
        levels=field_levels,
        title="Low ECS",
    )

    im_high = _plot_regular_latlon_panel(
        ax[0, 2],
        high_map,
        cmap=cmap,
        vmin=field_min,
        vmax=field_max,
        levels=field_levels,
        title="High ECS",
    )

    # ------------------------------------------------------------------
    # Bottom row: model - obs
    # ------------------------------------------------------------------
    im_default_diff = _plot_regular_latlon_panel(
        ax[1, 0],
        default_diff,
        cmap=diff_cmap_white,
        norm=diff_norm,
        levels=diff_levels,
        title="Default - Obs",
    )

    im_low_diff = _plot_regular_latlon_panel(
        ax[1, 1],
        low_diff,
        cmap=diff_cmap_white,
        norm=diff_norm,
        levels=diff_levels,
        title="Low ECS - Obs",
    )

    im_high_diff = _plot_regular_latlon_panel(
        ax[1, 2],
        high_diff,
        cmap=diff_cmap_white,
        norm=diff_norm,
        levels=diff_levels,
        title="High ECS - Obs",
    )

    # ------------------------------------------------------------------
    # Mean-value annotations
    # ------------------------------------------------------------------
    _annotate_panel_mean(ax[0, 0], default_mean, units=units)
    _annotate_panel_mean(ax[0, 1], low_mean, units=units)
    _annotate_panel_mean(ax[0, 2], high_mean, units=units)

    _annotate_panel_mean(ax[1, 0], default_diff_mean, units=units)
    _annotate_panel_mean(ax[1, 1], low_diff_mean, units=units)
    _annotate_panel_mean(ax[1, 2], high_diff_mean, units=units)

    # ------------------------------------------------------------------
    # Colorbars
    # ------------------------------------------------------------------
    label = var_name
    if units:
        label += f" [{units}]"

    cax_field = fig.add_axes([0.25, 0.505, 0.50, 0.024])
    cb_field = fig.colorbar(
        im_high,
        cax=cax_field,
        orientation="horizontal",
        extend="both",
    )
    cb_field.set_label(label)

    cax_diff = fig.add_axes([0.25, 0.055, 0.50, 0.024])
    cb_diff = fig.colorbar(
        im_high_diff,
        cax=cax_diff,
        orientation="horizontal",
        extend="both",
    )
    cb_diff.set_label(f"{label} difference from CERES-EBAF")

    title = (
        f"{var_name}: model ensemble means, {START_YEAR}-{END_YEAR}; "
        "CERES-EBAF obs, 2001-2018 climatology"
    )

    if MONTHS is not None:
        title += f", months={MONTHS}"

    fig.suptitle(title, fontsize=16, y=0.985)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_file, bbox_inches="tight", dpi=dpi)
    plt.close(fig)

    print(f"Saved {out_file}")

    return fig, ax


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading v3.LR.historical ...")
    ds_default, members_default = load_large_ensemble(
        ENSEMBLE_PREFIXES["default"],
        MEMBER_IDS,
        VARIABLES,
        XLE_PARENT_DIR,
        start_year=START_YEAR,
        end_year=END_YEAR,
        months=MONTHS,
        average_time=True,
    )

    print("\nLoading v3.LR.highECS.historical ...")
    ds_highECS, members_highECS = load_large_ensemble(
        ENSEMBLE_PREFIXES["highECS"],
        MEMBER_IDS,
        VARIABLES,
        XLE_PARENT_DIR,
        start_year=START_YEAR,
        end_year=END_YEAR,
        months=MONTHS,
        average_time=True,
    )

    print("\nLoading v3.LR.lowECS.historical ...")
    ds_lowECS, members_lowECS = load_large_ensemble(
        ENSEMBLE_PREFIXES["lowECS"],
        MEMBER_IDS,
        VARIABLES,
        XLE_PARENT_DIR,
        start_year=START_YEAR,
        end_year=END_YEAR,
        months=MONTHS,
        average_time=True,
    )

    if ds_default is None or ds_highECS is None or ds_lowECS is None:
        raise RuntimeError("One or more ensembles failed to load.")

    print("\nSummary")
    print(f"  default : {len(members_default)} members — {members_default}")
    print(f"  highECS : {len(members_highECS)} members — {members_highECS}")
    print(f"  lowECS  : {len(members_lowECS)} members — {members_lowECS}")

    for var_name in VARIABLES:
        out_file = OUT_DIR / f"{var_name}_model_obs_6panel_map_{START_YEAR}_{END_YEAR}.png"

        plot_model_obs_6panel_map(
            ds_default=ds_default,
            ds_low=ds_lowECS,
            ds_high=ds_highECS,
            var_name=var_name,
            out_file=out_file,
            obs_file=OBS_FILE,
            coeff=1.0,
            cmap="viridis",
            cmap_diff="RdBu_r",
            figsize=(17.0, 8.2),
            dpi=300,
        )


if __name__ == "__main__":
    main()