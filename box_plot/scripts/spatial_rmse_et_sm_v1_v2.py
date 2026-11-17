#!/usr/bin/env python3
"""LAI/ET/SM spatial RMSE; E3SM v1/v2 benchmark triangles on ET/SM only.

Reuse saved baseline climatological maps, recomputing all spatial scores on
their fixed land mask intersected with monthly-valid v1 and v2 cells. All
inputs must cover the entire existing matched period. Outputs are separate.
"""
import argparse
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

from land_common import AREA, LAT, LON, UNITS, dates, file_for, land_mask, monthly_blocks, time_axis
from monthly_mean_boxplots import ROOT, HERE, GROUPS, COLORS
from spatial_rmse_boxplots import atomic_csv
from spatial_rmse_metrics import SEASONS, month_durations

VARIABLES = {'lai': 'LAI (m²/m²)', 'et': 'ET (mm/day)', 'mrsos': 'SM (kg/m²)'}
CASES = {'v1': '20180215.DECKv1b_H1.ne30_oEC.edison', 'v2': 'v2.LR.historical_0101'}


def source_record(path):
    stat = path.stat()
    return {'path': str(path.resolve()), 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns}


def climatology(path, variable, keys):
    """Stream duration-weighted climatologies and require validity every month."""
    before = source_record(path)
    durations, calendar = month_durations(path, keys)
    lookup = {int(k): i for i, k in enumerate(keys)}
    sums = np.zeros((len(SEASONS), len(LAT), len(LON)), dtype=np.float64)
    totals = np.zeros(len(SEASONS))
    valid = np.ones((len(LAT), len(LON)), dtype=bool)
    seen = []
    for axis, fields in monthly_blocks(path, variable, keys):
        seen.extend(map(int, axis))
        fields = fields.astype(np.float64)
        valid &= np.isfinite(fields).all(axis=0)
        weights = durations[[lookup[int(k)] for k in axis]]
        for i, months in enumerate(SEASONS.values()):
            use = np.isin(axis % 12 + 1, months)
            sums[i] += np.sum(fields[use] * weights[use, None, None], axis=0)
            totals[i] += weights[use].sum()
    if sorted(seen) != sorted(keys) or len(seen) != len(set(seen)):
        raise ValueError(f'{path}: missing or duplicate comparison months')
    if source_record(path) != before:
        raise ValueError(f'{path}: input changed during calculation')
    return sums / totals[:, None, None], valid, totals, calendar


def calculate(variable, args):
    baseline_path = args.baseline_dir / f'{variable}_spatial_rmse_fields.nc'
    comparison_path = args.comparison_dir / f'{variable}_matched_monthly_means.nc'
    with xr.open_dataset(comparison_path) as comparison:
        keys = time_axis(comparison)
    baseline_source = source_record(baseline_path)
    with xr.open_dataset(baseline_path) as ds:
        ds = ds.load()
    np.testing.assert_array_equal(time_axis(ds), keys)
    np.testing.assert_allclose(ds.lat.values, LAT)
    np.testing.assert_allclose(ds.lon.values, LON)
    if list(ds.season.values) != list(SEASONS):
        raise ValueError('Unexpected baseline season order')
    original_mask = ds.comparison_mask.values.astype(bool)
    mask = original_mask.copy()
    new = []
    audit = {'baseline': baseline_source, 'new_inputs': {}}
    for version, case in CASES.items():
        folder = getattr(args, f'{version}_dir')
        path = file_for(folder, variable)
        if path is None:
            raise FileNotFoundError(f'{folder}: no {variable} input')
        maps, valid, weights, calendar = climatology(path, variable, keys)
        mask &= valid
        new.append((version, case, path, maps, weights, calendar))
        lost = np.argwhere(original_mask & ~valid)
        audit['new_inputs'][version] = {
            **source_record(path), 'calendar': calendar,
            'missing_baseline_cells': [{'lat': float(LAT[y]), 'lon': float(LON[x])} for y, x in lost],
        }
    if not mask.any():
        raise ValueError(f'{variable}: empty shared land domain')
    coverage = float(AREA[mask].sum() / AREA[land_mask()].sum())
    audit.update(variable=variable, months=len(keys), baseline_cells=int(original_mask.sum()),
                 comparison_cells=int(mask.sum()), removed_cells=int((original_mask & ~mask).sum()),
                 land_coverage_fraction=coverage)
    reference = ds.reference_climatology.values
    baseline_bias = ds.climatology_bias.values
    new_bias = np.stack([item[3] - reference for item in new])
    all_bias = np.concatenate([baseline_bias, new_bias], axis=0)
    if not np.isfinite(all_bias[:, :, mask]).all():
        raise ValueError(f'{variable}: missing climatological data on shared land domain')
    scores = np.sqrt(np.average(all_bias[:, :, mask] ** 2, axis=-1, weights=AREA[mask]))
    # An unchanged mask must give exactly the earlier baseline RMSE values.
    if np.array_equal(mask, original_mask):
        for i, season in enumerate(SEASONS):
            np.testing.assert_allclose(scores[:-2, i], ds[f'climatology_{season}'].values,
                                       rtol=1e-12, atol=1e-12)
    all_bias[:, :, ~mask] = np.nan
    names = list(map(str, ds.member.values)) + [item[1] for item in new]
    groups = list(map(str, ds.group.values)) + ['E3SMv1', 'E3SMv2']
    calendars = list(map(str, ds.model_calendar.values)) + [item[5] for item in new]
    weights = np.concatenate([ds.model_climatology_weight_days.values, np.stack([item[4] for item in new])])
    with (args.baseline_dir / f'spatial_rmse_{variable}.csv').open() as handle:
        old_records = list(csv.DictReader(handle))
    sources = {r['member']: r['source'] for r in old_records}
    for item in new:
        sources[item[1]] = str(item[2].resolve())
    if len(old_records) != len(ds.member) or set(sources) != set(names):
        raise ValueError(f'{variable}: baseline map/CSV manifest mismatch')
    records = []
    for j, (name, group) in enumerate(zip(names, groups)):
        row = dict(variable=variable, plot_label='ET' if variable == 'et' else 'SM', group=group,
                   member=name, plot_role='marker' if group in ['E3SMv1', 'E3SMv2'] else 'box',
                   units=UNITS[variable], observation=ds.attrs['observation'],
                   first_month=str(dates(keys)[0])[:7], last_month=str(dates(keys)[-1])[:7],
                   months=len(keys), land_coverage_fraction=coverage,
                   source=sources[name], model_calendar=calendars[j],
                   reference_calendar=ds.attrs['reference_calendar'])
        row.update({f'rmse_{s}': float(scores[j, i]) for i, s in enumerate(SEASONS)})
        records.append(row)
    output = xr.Dataset({
        'climatology_bias': (('member', 'season', 'lat', 'lon'), all_bias, {'units': UNITS[variable]}),
        'reference_climatology': (('season', 'lat', 'lon'), np.where(mask[None], reference, np.nan), {'units': UNITS[variable]}),
        'comparison_mask': (('lat', 'lon'), mask.astype('int8')),
        'baseline_mask': (('lat', 'lon'), original_mask.astype('int8')),
        'cell_area': (('lat', 'lon'), AREA, {'units': 'km2'}),
        'group': ('member', groups), 'model_calendar': ('member', calendars),
        'model_climatology_weight_days': (('member', 'season'), weights, {'units': 'days'}),
        'reference_climatology_weight_days': ('season', ds.reference_climatology_weight_days.values, {'units': 'days'}),
        'spatial_rmse': (('member', 'season'), scores, {'units': UNITS[variable]}),
    }, coords={'member': names, 'season': list(SEASONS), 'lat': LAT, 'lon': LON,
               'time': dates(keys)}, attrs={
        'observation': ds.attrs['observation'], 'variable': variable,
        'land_coverage_fraction': coverage, 'baseline_file': str(baseline_path.resolve()),
        'definition': 'sqrt(area_mean((duration_weighted_model_climatology - duration_weighted_reference_climatology)^2))',
        'domain': 'Baseline fixed land domain intersected with both v1/v2 valid cells across all matched months',
        'temporal_weighting': ds.attrs['temporal_weighting'],
        'reference_calendar': ds.attrs['reference_calendar'],
        'upstream_reference_commit': ds.attrs['upstream_reference_commit'],
    })
    target = args.output_dir / f'{variable}_spatial_rmse_fields.nc'
    tmp = target.with_name(target.stem + f'.{os.getpid()}.tmp.nc')
    output.to_netcdf(tmp, encoding={v: {'zlib': True, 'complevel': 1} for v in ['climatology_bias', 'reference_climatology']})
    tmp.replace(target)
    if source_record(baseline_path) != baseline_source:
        raise ValueError(f'{baseline_path}: baseline changed during calculation')
    atomic_csv(args.output_dir / f'spatial_rmse_{variable}.csv', records)
    (args.output_dir / f'{variable}_provenance.json').write_text(json.dumps(audit, indent=2) + '\n')
    print(f'{variable}: {len(records)} members/models; {len(keys)} months; removed {audit["removed_cells"]} baseline cells', flush=True)
    for row in records[-2:]:
        print(f'  {row["group"]}: ANN RMSE={row["rmse_ANN"]:.8g} {row["units"]}', flush=True)
    return records


def load_lai(args):
    """Carry the unchanged baseline LAI scores into the shared plotting table."""
    with (args.baseline_dir / 'spatial_rmse_lai.csv').open() as handle:
        baseline = list(csv.DictReader(handle))
    records = []
    for row in baseline:
        record = {key: row[key] for key in ['variable', 'group', 'member', 'units',
                  'observation', 'first_month', 'last_month', 'months',
                  'land_coverage_fraction', 'source', 'model_calendar', 'reference_calendar']}
        record.update(plot_label='LAI', plot_role='box')
        record.update({f'rmse_{s}': row[f'climatology_{s}'] for s in SEASONS})
        records.append(record)
    atomic_csv(args.output_dir / 'spatial_rmse_lai.csv', records)
    return records


def plot(records, args, season):
    fig, axes = plt.subplots(1, 3, figsize=(20, 6.2))
    groups = [g for g, _ in GROUPS] + ['CMIP6']
    handles = [Patch(facecolor=c, alpha=.7, label='E3SM ' + g) for c, (g, _) in zip(COLORS, GROUPS)]
    handles += [Patch(facecolor='white', edgecolor='.3', label='CMIP6')]
    handles += [Line2D([], [], marker='o', linestyle='', color=c, markeredgecolor='black', label='Mean ' + g)
                for c, (g, _) in zip(COLORS, GROUPS)]
    handles += [Line2D([], [], marker=m, linestyle='', color='black', markersize=10, label=g)
                for g, m in [('E3SMv1', '>'), ('E3SMv2', '<')]]
    outliers = []
    metric = f'rmse_{season}'
    for ax, (variable, title) in zip(axes, VARIABLES.items()):
        subset = [r for r in records if r['variable'] == variable]
        right = ax.twinx()
        for i, group in enumerate(groups):
            members = [r for r in subset if r['group'] == group]
            values = [float(r[metric]) for r in members]
            if not values:
                raise ValueError(f'{variable}: no {group} values')
            target = ax if i < 3 else right
            box = target.boxplot([values], positions=[i + 1], widths=.5, patch_artist=True,
                whis=[0, 100] if args.whiskers == 'range' else 1.5,
                boxprops={'edgecolor': '.3', 'linewidth': 1.4},
                medianprops={'color': '#ff7f0e', 'linewidth': 1.7},
                flierprops={'marker': 'o', 'markerfacecolor': 'none', 'markersize': 6})
            box['boxes'][0].set_facecolor(COLORS[i] if i < 3 else 'white')
            box['boxes'][0].set_alpha(.7 if i < 3 else 1)
            if i < 3:
                right.scatter(3.9 + .1 * i, np.mean(values), color=COLORS[i], edgecolors='black', s=55, zorder=6)
            q1, q3 = np.percentile(values, [25, 75])
            low, high = (min(values), max(values)) if args.whiskers == 'range' else (q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1))
            for row, value in zip(members, values):
                if value < low or value > high:
                    outliers.append({**row, 'plotted_season': season, 'plotted_value': value,
                                     'direction': 'low' if value < low else 'high', 'lower_fence': low, 'upper_fence': high})
        benchmarks = [] if variable == 'lai' else [('E3SMv1', 3.8, '>'), ('E3SMv2', 4.2, '<')]
        for group, x, marker in benchmarks:
            benchmark = [r for r in subset if r['group'] == group]
            if len(benchmark) != 1:
                raise ValueError(f'{variable}: expected one {group} benchmark')
            right.scatter(x, float(benchmark[0][metric]), marker=marker, color='black', s=150, zorder=7)
        ax.set_title(title, fontsize=18, fontweight='bold', pad=12)
        ax.set_ylabel('RMSE(E3SM configs)', fontsize=14)
        right.set_ylabel('RMSE(CMIP6)', fontsize=14)
        ax.set_xticks([1, 2, 3, 4], groups, rotation=16)
        ax.set_xlim(.5, 4.5)
        ax.tick_params(axis='both', labelsize=13)
        right.tick_params(axis='y', labelsize=13)
        ax.axvline(3.5, color='.7', linestyle='--', lw=.8)
        ax.grid(axis='y', linestyle='--', alpha=.4)
        ax.set_ylim(bottom=max(0, ax.get_ylim()[0]))
        right.set_ylim(bottom=max(0, right.get_ylim()[0]))
    fig.suptitle(f'{season} climatological spatial RMSE', fontsize=17, fontweight='bold', y=.99)
    fig.legend(handles=handles, loc='lower center', ncol=4, fontsize=11)
    fig.subplots_adjust(left=.06, right=.94, bottom=.29, top=.81, wspace=.67)
    suffix = '_range' if args.whiskers == 'range' else ''
    stem = f'spatial_rmse_et_sm_v1_v2_{season}{suffix}'
    for ext in ['png', 'pdf']:
        fig.savefig(args.output_dir / f'{stem}.{ext}', dpi=220, bbox_inches='tight')
    plt.close(fig)
    target = args.output_dir / f'outliers_{season}{suffix}.csv'
    if outliers:
        atomic_csv(target, outliers)
    else:
        with target.open('w', newline='') as handle:
            csv.writer(handle).writerow(list(records[0]) + ['plotted_season', 'plotted_value', 'direction', 'lower_fence', 'upper_fence'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--comparison-dir', type=Path, default=HERE / 'output')
    parser.add_argument('--baseline-dir', type=Path, default=HERE / 'output/spatial_rmse')
    parser.add_argument('--output-dir', type=Path, default=HERE / 'output/spatial_rmse_et_sm_v1_v2')
    for version in CASES:
        parser.add_argument(f'--{version}-dir', type=Path)
    parser.add_argument('--season', choices=['all'] + list(SEASONS), default='all')
    parser.add_argument('--whiskers', choices=['iqr', 'range'], default='iqr')
    parser.add_argument('--plot-only', action='store_true')
    args = parser.parse_args()
    for version, case in CASES.items():
        if getattr(args, f'{version}_dir') is None:
            setattr(args, f'{version}_dir', args.root / f'e3sm/output/{version}/{case}/zppy/post/lnd/180x360_aave/cmip_ts/monthly')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    records = []
    for variable in VARIABLES:
        if variable == 'lai':
            records.extend(load_lai(args))
        elif args.plot_only:
            with (args.output_dir / f'spatial_rmse_{variable}.csv').open() as handle:
                records.extend(csv.DictReader(handle))
        else:
            records.extend(calculate(variable, args))
    atomic_csv(args.output_dir / 'spatial_rmse_plot_data.csv', records)
    atomic_csv(args.output_dir / 'v1_v2_rmse.csv', [r for r in records if r['group'] in ['E3SMv1', 'E3SMv2']])
    if not args.plot_only:
        scripts = [Path(__file__), HERE / 'land_common.py', HERE / 'spatial_rmse_metrics.py']
        (args.output_dir / 'implementation.json').write_text(json.dumps(
            {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest() for p in scripts}, indent=2) + '\n')
    for season in SEASONS if args.season == 'all' else [args.season]:
        plot(records, args, season)
    print(f'Saved figures and CSVs in {args.output_dir}', flush=True)


if __name__ == '__main__':
    main()
