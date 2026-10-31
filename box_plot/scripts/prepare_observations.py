#!/usr/bin/env python3
"""Inventory ILAMB LAI/ET/mrsos; compute monthly global land means and coverage."""
import argparse
from concurrent.futures import ProcessPoolExecutor
import csv
import json
import shutil
from pathlib import Path
import numpy as np
import xarray as xr
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from land_common import DATA, LAT, LON, AREA, UNITS, land_mask, monthly_blocks, weighted_means, dates

HERE = Path(__file__).resolve().parent
FOLDERS = {'lai': 'lai', 'et': 'evspsbl', 'mrsos': 'mrsos'}


def analyze(task):
    path, variable, output, data_root, start, end = task
    product = path.parent.name
    tag = f'{variable}_{product}'
    land = land_mask(data_root)
    total_area = AREA[land].sum()
    count = np.zeros((180, 360), dtype=np.int32)
    overlap_count = np.zeros_like(count)
    keys, means, coverage = [], [], []
    for axis, fields in monthly_blocks(path, variable):
        mean, area = weighted_means(fields, land)
        keys.extend(axis.tolist()); means.extend(mean.tolist()); coverage.extend((area / total_area).tolist())
        count += np.sum(np.isfinite(fields), axis=0)
        overlap_count += np.sum(np.isfinite(fields[(axis >= start * 12) & (axis < (end + 1) * 12)]), axis=0)
    keys = np.asarray(keys)
    order = np.argsort(keys)
    keys = keys[order]
    means, coverage = np.asarray(means)[order], np.asarray(coverage)[order]
    overlap = (keys >= start * 12) & (keys < (end + 1) * 12)
    if len(np.unique(keys)) != len(keys):
        raise ValueError(f'Duplicate months: {path}')
    fraction = count / len(keys)
    full = overlap_count == overlap.sum() if overlap.any() else np.zeros_like(land)
    dataset = xr.Dataset({
        'global_monthly_mean': ('time', means, {'units': UNITS[variable], 'long_name': 'Area-weighted mean over available global land cells'}),
        'land_coverage_fraction': ('time', coverage, {'units': '1', 'long_name': 'Valid area divided by ILAMB global land area excluding Antarctica'}),
        'valid_month_fraction': (('lat', 'lon'), fraction, {'units': '1'}),
        'comparison_valid_month_fraction': (('lat', 'lon'), overlap_count / max(1, overlap.sum()), {'units': '1'}),
        'land_mask': (('lat', 'lon'), land.astype('int8')),
        'cell_area': (('lat', 'lon'), AREA, {'units': 'km2'}),
    }, coords={'time': dates(keys), 'lat': LAT, 'lon': LON}, attrs={
        'source_file': str(path), 'product': product, 'variable': variable,
        'land_mask_file': str(data_root / 'regions/GlobalLandNoAnt.nc'),
        'comparison_requested': f'{start}-{end}',
        'method': '0.5-degree cells area-averaged to 1 degree; coarse cells mapped by footprint. Means use finite cells within ILAMB GlobalLandNoAnt. Denominator varies by month; coverage is supplied.',
        'missing_values': 'CF masks, absolute values >= 1e19, native sentinel codes -999/-9999/-10000/-99999, and negative LAI/soil moisture are excluded.',
    })
    dataset.to_netcdf(output / 'observations' / (tag + '_monthly_mean.nc'))
    with (output / 'observations' / (tag + '_monthly_mean.csv')).open('w', newline='') as f:
        w = csv.writer(f); w.writerow(['month', 'global_monthly_mean', 'units', 'land_coverage_fraction'])
        w.writerows([(f'{k // 12:04d}-{k % 12 + 1:02d}', m, UNITS[variable], c) for k, m, c in zip(keys, means, coverage)])
    south = land & (LAT[:, None] < 0)
    north = land & (LAT[:, None] >= 0)
    union = overlap_count > 0
    row = dict(variable=variable, product=product, source=str(path),
               first_month=f'{keys[0] // 12:04d}-{keys[0] % 12 + 1:02d}',
               last_month=f'{keys[-1] // 12:04d}-{keys[-1] % 12 + 1:02d}',
               months=len(keys), overlap_months=int(overlap.sum()),
               median_land_coverage=float(np.median(coverage[overlap])) if overlap.any() else 0.,
               persistent_land_coverage=float(AREA[full & land].sum() / total_area),
               northern_land_coverage=float(AREA[union & north].sum() / AREA[north].sum()),
               southern_land_coverage=float(AREA[union & south].sum() / AREA[south].sum()))
    print(f'{tag}: {row["first_month"]}..{row["last_month"]}, median global land coverage={row["median_land_coverage"]:.1%}', flush=True)
    return row


def visualize(rows, output):
    datasets = [xr.open_dataset(output / 'observations' / f'{r["variable"]}_{r["product"]}_monthly_mean.nc') for r in rows]
    min_key = min(int(d.time.dt.year.min()) * 12 for d in datasets)
    max_key = max(int(d.time.dt.year.max()) * 12 + 11 for d in datasets)
    timeline = np.full((len(rows), max_key - min_key + 1), np.nan)
    for i, d in enumerate(datasets):
        key = np.asarray(d.time.dt.year * 12 + d.time.dt.month - 1, dtype=int)
        timeline[i, key - min_key] = d.land_coverage_fraction.values * 100
    fig, ax = plt.subplots(figsize=(15, 7))
    cmap = plt.get_cmap('viridis').copy(); cmap.set_bad('#eeeeee')
    im = ax.imshow(np.ma.masked_invalid(timeline), origin='upper', aspect='auto', cmap=cmap, vmin=0, vmax=100,
                   extent=(min_key / 12, (max_key + 1) / 12, len(rows) - .5, -.5))
    ax.set_yticks(range(len(rows)), [f'{r["variable"]} — {r["product"]}' for r in rows])
    ax.set_xlabel('Year (monthly records; gray = no record)')
    ax.set_title('ILAMB observation availability and monthly global land coverage')
    fig.colorbar(im, ax=ax, label='Valid area / global land area excluding Antarctica (%)')
    fig.tight_layout(); fig.savefig(output / 'observation_time_availability.png', dpi=180); plt.close(fig)
    fig, axes = plt.subplots(4, 3, figsize=(18, 12), constrained_layout=True)
    map_cmap = plt.get_cmap('viridis').copy(); map_cmap.set_bad('white')
    for ax, row, d in zip(axes.flat, rows, datasets):
        fraction = d.valid_month_fraction.where(d.valid_month_fraction > 0) * 100
        im = ax.pcolormesh(LON, LAT, fraction, vmin=0, vmax=100, cmap=map_cmap, shading='nearest')
        ax.set(xlim=(-180, 180), ylim=(-90, 90), xticks=[-180, -90, 0, 90, 180], yticks=[-60, 0, 60])
        ax.set_title(f'{row["variable"]} — {row["product"]}\n{row["first_month"]} to {row["last_month"]}; land coverage {row["median_land_coverage"]:.0%}', fontsize=10)
        ax.set_xlabel('Longitude'); ax.set_ylabel('Latitude')
    for ax in list(axes.flat)[len(rows):]: ax.set_visible(False)
    fig.colorbar(im, ax=list(axes.flat), label='Months with valid data at each grid cell (%)', shrink=.7)
    fig.suptitle('Spatial data availability over each product’s full record; white = no data', fontsize=16)
    fig.savefig(output / 'observation_spatial_coverage.png', dpi=160); plt.close(fig)
    fig, axes = plt.subplots(3, 1, figsize=(15, 10), sharex=True, constrained_layout=True)
    for ax, variable in zip(axes, FOLDERS):
        for row, d in zip(rows, datasets):
            if row['variable'] == variable:
                ax.plot(d.time.values, d.global_monthly_mean.values, label=row['product'], lw=.8)
        ax.set_ylabel(f'{variable} ({UNITS[variable]})'); ax.legend(ncol=3, fontsize=9)
        ax.grid(alpha=.25)
    axes[0].set_title('Area-weighted monthly means of the available global land observations')
    fig.savefig(output / 'observation_global_monthly_means.png', dpi=180); plt.close(fig)
    for d in datasets: d.close()


def select(rows, output):
    selected = {}
    for variable in FOLDERS:
        candidates = [r for r in rows if r['variable'] == variable and r['usable_overlap_months'] >= 12]
        # Global land products must represent both hemispheres. CARDAMOM is a
        # model-data assimilation product and is audited but not selected by default.
        eligible = [r for r in candidates if r['product'] != 'CARDAMOM'
                    and r['northern_land_coverage'] >= .5 and r['southern_land_coverage'] >= .5]
        if not eligible:
            raise ValueError(f'{variable}: no sufficiently global product; inspect inventory before proceeding')
        # Prefer the longest matched record among global products; use stable
        # spatial coverage to choose between products with the same time span.
        best = max(eligible, key=lambda r: (r['usable_overlap_months'], r['persistent_land_coverage']))
        selected[variable] = {**best, 'selection_reason': 'Most overlapping months with usable data among non-CARDAMOM products representing >=50% of land in each hemisphere; highest persistent global land coverage breaks ties.'}
    (output / 'selected_observations.json').write_text(json.dumps(selected, indent=2) + '\n')
    return selected


def write_selected_series(selected, output):
    records, datasets = [], []
    for variable, row in selected.items():
        path = output / 'observations' / f'{variable}_{row["product"]}_monthly_mean.nc'
        with xr.open_dataset(path) as ds:
            a = ds[['global_monthly_mean', 'land_coverage_fraction']].load()
            for time, mean, fraction in zip(a.time.values, a.global_monthly_mean.values, a.land_coverage_fraction.values):
                records.append(dict(variable=variable, product=row['product'], month=str(time)[:7],
                                    global_monthly_mean=float(mean), units=UNITS[variable],
                                    land_coverage_fraction=float(fraction)))
        a = a.rename({'global_monthly_mean': variable, 'land_coverage_fraction': variable + '_land_coverage_fraction'})
        a.attrs = {}
        a[variable].attrs['observation_product'] = row['product']
        datasets.append(a)
    merged = xr.merge(datasets, join='outer', compat='no_conflicts')
    merged.attrs['method'] = 'Area-weighted means over each selected product available global land cells; see coverage variables. Antarctica excluded.'
    merged.to_netcdf(output / 'selected_observation_monthly_means.nc')
    with (output / 'selected_observation_monthly_means.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0])); writer.writeheader(); writer.writerows(records)


def save_selected_fields(selected, output, start, end):
    for variable, row in selected.items():
        arrays, keys = [], []
        wanted = set(range(start * 12, (end + 1) * 12))
        for axis, fields in monthly_blocks(Path(row['source']), variable, wanted):
            keys.extend(axis.tolist()); arrays.append(fields)
        order = np.argsort(keys)
        ds = xr.Dataset({'field': (('time', 'lat', 'lon'), np.concatenate(arrays)[order], {'units': UNITS[variable]})},
                        coords={'time': dates(np.asarray(keys)[order]), 'lat': LAT, 'lon': LON},
                        attrs={'source_file': row['source'], 'product': row['product'], 'variable': variable})
        path = output / 'observations' / f'{variable}_selected_monthly_fields.nc'
        temporary = path.with_suffix('.tmp.nc')
        ds.to_netcdf(temporary, encoding={'field': {'zlib': True, 'complevel': 1, 'dtype': 'float32'}})
        temporary.replace(path)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data-root', type=Path, default=DATA)
    p.add_argument('--output', type=Path, default=HERE / 'output')
    p.add_argument('--start-year', type=int, default=1985); p.add_argument('--end-year', type=int, default=2014)
    p.add_argument('--jobs', type=int, default=3)
    p.add_argument('--skip-fields', action='store_true', help='Update summaries and selection without exporting selected maps.')
    p.add_argument('--reuse-inventory', action='store_true', help='Reuse completed observation summaries and update selection/plots.')
    args = p.parse_args()
    (args.output / 'observations').mkdir(parents=True, exist_ok=True)
    tasks = [(f, v, args.output, args.data_root, args.start_year, args.end_year)
             for v, folder in FOLDERS.items() for f in sorted((args.data_root / folder).glob('*/*.nc'))]
    if not tasks: raise FileNotFoundError(args.data_root)
    if args.reuse_inventory:
        with (args.output / 'observation_inventory.csv').open() as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            for key in ['months', 'overlap_months']: row[key] = int(row[key])
            for key in ['median_land_coverage', 'persistent_land_coverage', 'northern_land_coverage', 'southern_land_coverage']: row[key] = float(row[key])
    else:
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            rows = list(pool.map(analyze, tasks))
    for row in rows:
        path = args.output / 'observations' / f'{row["variable"]}_{row["product"]}_monthly_mean.nc'
        with xr.open_dataset(path) as ds:
            good = np.isfinite(ds.global_monthly_mean.values) & (ds.land_coverage_fraction.values > 0)
            year = ds.time.dt.year.values
            overlap = (year >= args.start_year) & (year <= args.end_year)
            row['usable_overlap_months'] = int(np.sum(good & overlap))
            good_dates = ds.time.values[good]
            row['first_usable_month'] = str(good_dates[0])[:7] if len(good_dates) else ''
            row['last_usable_month'] = str(good_dates[-1])[:7] if len(good_dates) else ''
            row['no_data_months'] = ';'.join(str(t)[:7] for t in ds.time.values[~good])
    with (args.output / 'observation_inventory.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    visualize(rows, args.output)
    selected = select(rows, args.output)
    print('Selected:', {v: r['product'] for v, r in selected.items()}, flush=True)
    write_selected_series(selected, args.output)
    for variable, row in selected.items():
        for ext in ['nc', 'csv']:
            shutil.copy2(args.output / 'observations' / f'{variable}_{row["product"]}_monthly_mean.{ext}', args.output / f'{variable}_observation_global_monthly_mean.{ext}')
    if not args.skip_fields:
        save_selected_fields(selected, args.output, args.start_year, args.end_year)


if __name__ == '__main__': main()
