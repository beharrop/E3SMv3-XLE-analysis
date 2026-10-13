#!/usr/bin/env python3

import argparse
import re

import numpy as np
import regionmask
import xarray as xr


def safe_name(text):
    """Convert text to a simple NetCDF-friendly variable-name component."""
    text = re.sub(r"[^A-Za-z0-9_]+", "_", str(text)).strip("_")
    return text or "unnamed"


def main():
    parser = argparse.ArgumentParser(
        description="Create center-point SREX region masks on an E3SM SCRIP grid."
    )
    parser.add_argument(
        "--grid",
        default="/global/cfs/cdirs/e3sm/mapping/grids/ne30pg2_scrip_20200209.nc",
        help="Input SCRIP grid file.",
    )
    parser.add_argument(
        "--output",
        default="ne30pg2_srex_masks.nc",
        help="Output NetCDF file.",
    )
    args = parser.parse_args()

    # Read the cell-center coordinates. grid_imask is intentionally not used.
    with xr.open_dataset(args.grid) as grid:
        lon_original = np.asarray(grid["grid_center_lon"].values, dtype=float)
        lat = np.asarray(grid["grid_center_lat"].values, dtype=float)

    if lon_original.ndim != 1 or lat.ndim != 1:
        raise ValueError("Expected grid_center_lon and grid_center_lat to be 1-D.")
    if lon_original.shape != lat.shape:
        raise ValueError("Longitude and latitude arrays have different sizes.")

    # Normalize longitudes to [-180, 180), which is a convenient convention
    # for the SREX polygons.
    lon = (lon_original + 180.0) % 360.0 - 180.0

    grid_size = np.arange(lon.size)
    lon_da = xr.DataArray(lon, dims="grid_size", coords={"grid_size": grid_size})
    lat_da = xr.DataArray(lat, dims="grid_size", coords={"grid_size": grid_size})

    # Built-in regionmask SREX definition.
    srex = regionmask.defined_regions.srex

    # Keep all regions in the result, including any that happen to contain
    # no grid-cell centers.
    masks_3d = srex.mask_3D(lon_da, lat_da, drop=False)

    out = xr.Dataset(
        coords={
            "grid_size": grid_size,
            "grid_center_lon": (
                "grid_size",
                lon_original,
                {"units": "degrees_east"},
            ),
            "grid_center_lat": (
                "grid_size",
                lat,
                {"units": "degrees_north"},
            ),
        },
        attrs={
            "description": (
                "Center-point masks for the built-in regionmask SREX regions "
                "on the supplied SCRIP grid."
            ),
            "source_grid": args.grid,
            "mask_method": "A cell is in a region if its center is contained in it.",
            "grid_imask_used": "No; all grid cells are included.",
            "mask_encoding": "uint8 values: 0 = false, 1 = true",
        },
    )

    # Create one variable per SREX region. The region number is included in
    # each variable name to ensure names remain unique.
    for i, (number, abbrev, name) in enumerate(
        zip(srex.numbers, srex.abbrevs, srex.names)
    ):
        variable_name = f"srex_{number}_{safe_name(abbrev)}"
        values = masks_3d.isel(region=i).values.astype(np.uint8)

        out[variable_name] = xr.DataArray(
            values,
            dims=("grid_size",),
            attrs={
                "long_name": f"Center-point mask for SREX region {name}",
                "region_number": int(number),
                "region_abbreviation": str(abbrev),
                "region_name": str(name),
                "flag_values": np.array([0, 1], dtype=np.uint8),
                "flag_meanings": "not_in_region in_region",
            },
        )

    # Compress the mask variables in the output file.
    encoding = {
        name: {"zlib": True, "complevel": 4, "dtype": "uint8"}
        for name in out.data_vars
    }

    out.to_netcdf(
        args.output,
        engine="netcdf4",
        format="NETCDF4",
        encoding=encoding,
    )
    print(f"Wrote SREX masks to {args.output}")


if __name__ == "__main__":
    main()
