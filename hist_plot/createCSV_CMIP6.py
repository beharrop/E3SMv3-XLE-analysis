import glob
import os
import re

import numpy as np
import xarray as xr
import xcdat as xc

root = "/global/cfs/cdirs/m3522/datalake/CMIP6/CMIP"
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CMIP6_csv")


def discover_models(root):
    """Map each model name to its (modeling center, realization) pairs found under root."""
    models = {}
    paths = sorted(glob.glob(os.path.join(root, "*", "*", "historical", "r?i1p1f1")))
    for path in paths:
        center, model, _experiment, realization = os.path.relpath(path, root).split(os.sep)
        entries = models.setdefault(model, [])
        if entries and entries[0][0] != center:
            print(f"WARNING: {model} found under multiple centers ({entries[0][0]}, {center})")
        entries.append((center, realization))
    for model, entries in sorted(models.items()):
        print(f"{model}: {len(entries)} realization(s)")
    return models


def realization_number(realization):
    """Extract the ensemble number from a realization label like 'r3i1p1f1'."""
    return re.match(r"^r(\d+)i", realization).group(1)


def find_latest_files(root, center, model, realization):
    """Locate tas files for the latest available version, searching across grid labels."""
    base = os.path.join(root, center, model, "historical", realization, "Amon", "tas")
    for grid_dir in sorted(glob.glob(os.path.join(base, "*"))):
        version_dirs = sorted(glob.glob(os.path.join(grid_dir, "v*")))
        if not version_dirs:
            continue
        files = sorted(glob.glob(os.path.join(version_dirs[-1], "tas_Amon_*_*.nc")))
        if files:
            return files
    return []


def monthly_spatial_mean(ds_in, lat_bounds=(-90,90)):
    return ds_in.spatial.average(
        data_var="tas",
        axis=["X", "Y"],
        weights="generate",
        lat_bounds=lat_bounds,
    )

def annual_day_weighted_mean(ds_in, data_var="tas", weighted=True):
    return ds_in.temporal.group_average(data_var, freq="year", weighted=weighted)


def _tas_values(annual_mean):
    # spatial.average() returns a Dataset, but be tolerant of a bare DataArray too.
    return annual_mean["tas"].values if isinstance(annual_mean, xr.Dataset) else annual_mean.values


def process_realization(root, output_dir, center, model, realization):
    n = realization_number(realization)
    out_path = os.path.join(output_dir, f"{model}_r{n}.csv")
    incomplete_path = os.path.join(output_dir, f"{model}_r{n}_incomplete.csv")
    if os.path.exists(out_path) or os.path.exists(incomplete_path):
        print(f"Skipping {model} {realization}, output already exists")
        return

    files = find_latest_files(root, center, model, realization)
    if not files:
        print(f"WARNING: no tas files found for {model} {realization}, skipping")
        return

    print(f"Processing {model} {realization}")
    try:
        ds = xc.open_mfdataset(files, combine="by_coords", chunks={"time": 120}, lon_orient=(-180,180))
        ds = ds.bounds.add_missing_bounds()

        tas_global_annual = annual_day_weighted_mean(
            monthly_spatial_mean(ds, lat_bounds=(-90, 90))
        )
        tas_nh_annual = annual_day_weighted_mean(
            monthly_spatial_mean(ds, lat_bounds=(0, 90))
        )
        tas_sh_annual = annual_day_weighted_mean(
            monthly_spatial_mean(ds, lat_bounds=(-90, 0))
        )

        year = tas_global_annual['time'].dt.year.values
        data = np.column_stack(
            [
                year.astype("float64"),
                _tas_values(tas_global_annual),
                _tas_values(tas_nh_annual),
                _tas_values(tas_sh_annual),
            ]
        )
    except Exception as exc:
        print(f"WARNING: failed to process {model} {realization}: {exc}")
        return

    if len(year) != 165 or year.min() != 1850 or year.max() != 2014:
        print(f"NOTE: {model} {realization} spans {year.min()}-{year.max()} ({len(year)} years)")
        out_path = incomplete_path

    np.savetxt(out_path, data, delimiter=",")


def main():
    os.makedirs(output_dir, exist_ok=True)
    models = discover_models(root)
    for model, entries in sorted(models.items()):
        for center, realization in entries:
            process_realization(root, output_dir, center, model, realization)


if __name__ == "__main__":
    main()

