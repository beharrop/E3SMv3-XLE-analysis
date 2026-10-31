#!/usr/bin/env python3
"""Monthly global-land-mean time-series RMSE on matched months and land cells.

Run prepare_observations.py first. Both model and observation means are recomputed
on one fixed spatial domain per variable, shared by every included member/model.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor
import csv
import hashlib
import json
import os
from pathlib import Path
import numpy as np
import xarray as xr
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from land_common import AREA, LAT, LON, UNITS, land_mask, monthly_blocks, weighted_means, time_axis, dates, file_for

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
GROUPS = [('lowECS', 'lowECS'), ('Default', 'LR'), ('highECS', 'highECS')]
COLORS = ['#1f77b4', '#2ca02c', '#d62728']
TITLES = {'lai': 'LAI', 'et': 'ET', 'mrsos': 'Soil moisture (0–10 cm)'}


def inventory(root, variable, limit):
    rows = []
    for label, family in GROUPS:
        prefix = 'v3.LR.' + (family + '.' if family != 'LR' else '') + 'historical_'
        members = sorted((root / 'e3sm/output' / family).glob(prefix + '[0-9][0-9][0-9][0-9]'))
        if limit: members = members[:limit]
        if not members: raise FileNotFoundError(f'No {label} members')
        for member in members:
            path = file_for(member, variable)
            if path is None: raise FileNotFoundError(f'{member}: {variable}')
            rows.append((label, member.name, path))
    cmip = []
    for directory in sorted((root / 'cmip6/output').iterdir()):
        if directory.is_dir() and directory.name != 'cmip_ens':
            path = file_for(directory, variable)
            if path: cmip.append(('CMIP6', directory.name, path))
    if not cmip: raise FileNotFoundError(f'No CMIP6 {variable}')
    rows.extend(cmip[:limit] if limit else cmip)
    return rows


def available_months(path):
    with xr.open_dataset(path) as ds:
        return set(map(int, time_axis(ds)))


def cache_path(path, variable, keys, directory, stage, mask=None):
    stat = path.stat()
    signature = [str(path.resolve()), stat.st_size, stat.st_mtime_ns,
                 variable, sorted(map(int, keys)), stage, 'area-grid-v1']
    if mask is not None: signature.append(hashlib.sha256(mask.tobytes()).hexdigest())
    return directory / (hashlib.sha256(json.dumps(signature).encode()).hexdigest() + '.npz')


def save_cache(path, **data):
    tmp = path.with_name(path.name + f'.{os.getpid()}.tmp')
    with tmp.open('wb') as f: np.savez_compressed(f, **data)
    tmp.replace(path)


def support_task(task):
    path, variable, keys, directory = task
    cache = cache_path(path, variable, keys, directory, 'support')
    if cache.exists():
        with np.load(cache) as d: return d['mask']
    mask = np.ones((180, 360), dtype=bool)
    seen = set()
    for axis, fields in monthly_blocks(path, variable, keys):
        seen.update(map(int, axis)); mask &= np.all(np.isfinite(fields), axis=0)
    if seen != set(keys): raise ValueError(f'Missing months: {path}')
    save_cache(cache, mask=mask)
    return mask


def mean_task(task):
    path, variable, keys, directory, mask = task
    cache = cache_path(path, variable, keys, directory, 'mean', mask)
    if cache.exists():
        with np.load(cache) as d: return d['keys'], d['mean']
    axes, values = [], []
    for axis, fields in monthly_blocks(path, variable, keys):
        if not np.all(np.isfinite(fields[:, mask])): raise ValueError(f'Incomplete fixed domain: {path}')
        mean, _ = weighted_means(fields, mask)
        axes.extend(axis.tolist()); values.extend(mean.tolist())
    order = np.argsort(axes)
    axis, mean = np.asarray(axes)[order], np.asarray(values)[order]
    save_cache(cache, keys=axis, mean=mean)
    return axis, mean


def compute(args):
    selected = json.loads((args.output / 'selected_observations.json').read_text())
    all_rows, metadata = [], {}
    suffix = '_smoke' if args.limit else ''
    for variable, reference in selected.items():
        if args.variable and variable != args.variable: continue
        rows = inventory(args.root, variable, args.limit)
        obs_path = args.output / 'observations' / f'{variable}_selected_monthly_fields.nc'
        with xr.open_dataset(obs_path) as ds:
            obs = ds.load()
        if obs.attrs.get('product') != reference['product']:
            raise ValueError(f'{variable}: selected fields are stale; rerun prepare_observations.py')
        observation_keys = time_axis(obs)
        initial_keys = sorted(map(int, observation_keys))
        # Ignore records with no finite observed land cells.
        usable = np.any(np.isfinite(obs.field.values) & land_mask(args.data_root), axis=(1, 2))
        keys = set(map(int, observation_keys[usable]))
        # Calendar-month identifiers, rather than raw timestamps, join ELM
        # interval-end stamps to CMIP6 mid-month and observation timestamps.
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            for available in pool.map(available_months, [path for _, _, path in rows]):
                keys &= available
        keys = sorted(keys)
        if len(keys) < 12: raise ValueError(f'{variable}: fewer than 12 shared months')
        indices = np.flatnonzero(np.isin(time_axis(obs), keys))
        obs_fields = obs.field.values[indices]
        mask = land_mask(args.data_root) & np.all(np.isfinite(obs_fields), axis=0)
        tasks = [(path, variable, keys, args.cache_dir) for _, _, path in rows]
        print(f'{variable}: {len(keys)} common months, {len(rows)} members/models; building shared spatial domain', flush=True)
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            for (_, name, _), support in zip(rows, pool.map(support_task, tasks)):
                mask &= support
                print('  support:', name, flush=True)
        if not mask.any(): raise ValueError(f'{variable}: no shared land cells')
        coverage = float(AREA[mask].sum() / AREA[land_mask(args.data_root)].sum())
        print(f'  Fixed shared domain: {mask.sum()} cells, {coverage:.1%} of global land', flush=True)
        observed, _ = weighted_means(obs_fields, mask)
        timeseries = [observed]
        tasks = [(path, variable, keys, args.cache_dir, mask) for _, _, path in rows]
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            for (group, member, path), (axis, modeled) in zip(rows, pool.map(mean_task, tasks)):
                if not np.array_equal(axis, keys): raise ValueError(f'Month ordering mismatch: {path}')
                rmse = float(np.sqrt(np.mean((modeled - observed) ** 2)))
                bias = float(np.mean(modeled - observed))
                all_rows.append(dict(variable=variable, group=group, member=member, rmse=rmse, bias=bias,
                                     units=UNITS[variable], first_month=f'{keys[0] // 12:04d}-{keys[0] % 12 + 1:02d}',
                                     last_month=f'{keys[-1] // 12:04d}-{keys[-1] % 12 + 1:02d}', months=len(keys),
                                     land_coverage_fraction=coverage, observation=reference['product'], source=str(path)))
                timeseries.append(modeled)
                print('  monthly mean:', member, flush=True)
        labels = ['Observation'] + [member for _, member, _ in rows]
        groups = ['Observation'] + [group for group, _, _ in rows]
        result = xr.Dataset({
            'global_monthly_mean': (('series', 'time'), np.asarray(timeseries), {'units': UNITS[variable]}),
            'comparison_mask': (('lat', 'lon'), mask.astype('int8')),
            'group': ('series', groups),
        }, coords={'time': dates(keys), 'series': labels, 'lat': LAT, 'lon': LON}, attrs={
            'observation': reference['product'], 'source_observation': reference['source'],
            'land_coverage_fraction': coverage,
            'rmse_definition': 'sqrt(mean over shared calendar months of (model global monthly mean - observation global monthly mean)^2); equal month weights',
            'spatial_domain': 'Fixed intersection of finite land cells across every included observation month and model/member month on a 1-degree grid',
        })
        result.to_netcdf(args.output / f'{variable}_matched_monthly_means{suffix}.nc')
        metadata[variable] = dict(observation=reference['product'], months=keys,
                                  dropped_observation_months=sorted(set(initial_keys) - set(keys)),
                                  land_coverage_fraction=coverage, grid_cells=int(mask.sum()))
        part = [r for r in all_rows if r['variable'] == variable]
        with (args.output / f'monthly_mean_rmse_{variable}{suffix}.csv').open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(part[0])); writer.writeheader(); writer.writerows(part)
        (args.output / f'matched_periods_{variable}{suffix}.json').write_text(json.dumps(metadata[variable], indent=2) + '\n')
    if args.variable:
        all_rows, metadata = [], {}
        for variable in TITLES:
            part = args.output / f'monthly_mean_rmse_{variable}{suffix}.csv'
            if not part.exists(): return None
            with part.open() as f: all_rows.extend(csv.DictReader(f))
            metadata[variable] = json.loads((args.output / f'matched_periods_{variable}{suffix}.json').read_text())
    with (args.output / f'monthly_mean_rmse{suffix}.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(all_rows[0])); w.writeheader(); w.writerows(all_rows)
    (args.output / f'matched_periods{suffix}.json').write_text(json.dumps(metadata, indent=2) + '\n')
    return all_rows


def plot(args, rows):
    suffix = '_smoke' if args.limit else ''
    fig, axes = plt.subplots(1, 3, figsize=(20, 6.2))
    handles = [Patch(facecolor=c, alpha=.7, label='E3SM ' + g) for c, (g, _) in zip(COLORS, GROUPS)]
    handles += [Patch(facecolor='white', edgecolor='.3', label='CMIP6')]
    handles += [Line2D([], [], marker='o', color=c, markeredgecolor='black', linestyle='', label='Mean ' + g) for c, (g, _) in zip(COLORS, GROUPS)]
    for ax, variable in zip(axes, TITLES):
        subset = [r for r in rows if r['variable'] == variable]
        right = ax.twinx()
        groups = [g for g, _ in GROUPS] + ['CMIP6']
        for i, group in enumerate(groups):
            values = [float(r['rmse']) for r in subset if r['group'] == group]
            target = ax if i < 3 else right
            box = target.boxplot([values], positions=[i + 1], widths=.5, patch_artist=True,
                                 boxprops={'edgecolor': '.3', 'linewidth': 1.4},
                                 medianprops={'color': '#ff7f0e', 'linewidth': 1.7},
                                 flierprops={'marker': 'o', 'markerfacecolor': 'none', 'markersize': 6})
            box['boxes'][0].set_facecolor(COLORS[i] if i < 3 else 'white')
            box['boxes'][0].set_alpha(.7 if i < 3 else 1)
            if i < 3: right.scatter(3.90 + i * .10, np.mean(values), c=COLORS[i], edgecolors='black', s=55, zorder=5)
        first = subset[0]
        ax.set_title(f'{TITLES[variable]}\n{first["observation"]}; {first["first_month"]} to {first["last_month"]}', fontsize=13, pad=12)
        ax.set_ylabel(f'Monthly-mean RMSE (E3SM; {UNITS[variable]})')
        right.set_ylabel(f'Monthly-mean RMSE (CMIP6; {UNITS[variable]})')
        ax.set_xticks([1, 2, 3, 4], groups, rotation=16); ax.set_xlim(.5, 4.5)
        ax.axvline(3.5, color='.7', linestyle='--', lw=.8); ax.grid(axis='y', linestyle='--', alpha=.4)
        ax.set_ylim(bottom=max(0, ax.get_ylim()[0])); right.set_ylim(bottom=max(0, right.get_ylim()[0]))
    fig.legend(handles=handles, loc='lower center', ncol=4, fontsize=11)
    fig.subplots_adjust(left=.06, right=.94, bottom=.25, top=.81, wspace=.65)
    for ext in ['png', 'pdf']: fig.savefig(args.output / f'monthly_mean_rmse_boxplots{suffix}.{ext}', dpi=220, bbox_inches='tight')
    plt.close(fig)
    fig, axes = plt.subplots(3, 1, figsize=(15, 11), sharex=True, constrained_layout=True)
    for ax, variable in zip(axes, TITLES):
        with xr.open_dataset(args.output / f'{variable}_matched_monthly_means{suffix}.nc') as ds:
            ax.plot(ds.time, ds.global_monthly_mean.sel(series='Observation'), color='black', lw=1.3, label=ds.attrs['observation'])
            for color, (group, _) in zip(COLORS, GROUPS):
                mean = ds.global_monthly_mean.isel(series=np.flatnonzero(ds.group.values == group)).mean('series')
                ax.plot(ds.time, mean, color=color, lw=.8, label=group)
            cmip = ds.global_monthly_mean.isel(series=np.flatnonzero(ds.group.values == 'CMIP6')).mean('series')
            ax.plot(ds.time, cmip, color='.5', lw=.8, label='CMIP6 mean')
            ax.set_ylabel(f'{variable} ({UNITS[variable]})'); ax.legend(ncol=5, fontsize=9); ax.grid(alpha=.25)
    axes[0].set_title('Observation and ensemble global monthly means on identical months and land cells')
    fig.savefig(args.output / f'matched_global_monthly_means{suffix}.png', dpi=180); plt.close(fig)
    fig, axes = plt.subplots(1, 3, figsize=(16, 4), constrained_layout=True)
    for ax, variable in zip(axes, TITLES):
        with xr.open_dataset(args.output / f'{variable}_matched_monthly_means{suffix}.nc') as ds:
            ax.pcolormesh(LON, LAT, np.where(ds.comparison_mask, 1., np.nan), cmap='Greens', vmin=0, vmax=1, shading='nearest')
            ax.set_title(f'{TITLES[variable]}\n{ds.attrs["land_coverage_fraction"]:.1%} of global land')
            ax.set(xlim=(-180, 180), ylim=(-90, 90), xlabel='Longitude', ylabel='Latitude')
    fig.savefig(args.output / f'boxplot_comparison_domains{suffix}.png', dpi=180); plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=ROOT); p.add_argument('--output', type=Path, default=HERE / 'output')
    p.add_argument('--data-root', type=Path, default=Path('/global/cfs/cdirs/e3sm/feng809/data/ILAMB-DATA'))
    p.add_argument('--cache-dir', type=Path, default=HERE / '.cache_monthly'); p.add_argument('--jobs', type=int, default=3)
    p.add_argument('--variable', choices=list(TITLES), help='Compute one variable and save a checkpoint; plot after all three are ready')
    p.add_argument('--limit', type=int, help='Use N members/models per group for a smoke check')
    p.add_argument('--plot-only', action='store_true')
    args = p.parse_args(); args.cache_dir.mkdir(parents=True, exist_ok=True)
    suffix = '_smoke' if args.limit else ''
    if args.plot_only:
        with (args.output / f'monthly_mean_rmse{suffix}.csv').open() as f: rows = list(csv.DictReader(f))
    else: rows = compute(args)
    if rows is not None: plot(args, rows)
    else: print('Variable saved; run the other variables to generate the combined plots.', flush=True)


if __name__ == '__main__': main()
