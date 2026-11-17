#!/usr/bin/env python3
"""Separate spatial-RMSE workflow using the existing verified months/land domains.

Default: duration-weighted annual climatological maps, then area-weighted spatial RMSE,
following the E3SM Diagnostics lat_lon driver.
Also computes monthly-map and period-mean-map RMSE, with separate plot choices.
The monthly global-mean RMSE outputs are read for verification, never overwritten.
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
from land_common import AREA, LAT, LON, UNITS, time_axis, dates
from monthly_mean_boxplots import ROOT, HERE, GROUPS, COLORS, TITLES, inventory
from spatial_rmse_metrics import METRICS, SEASONS, model_metrics, month_durations, weighted_climatologies


def atomic_csv(path, rows):
    tmp = path.with_name(path.name + f'.{os.getpid()}.tmp')
    with tmp.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    tmp.replace(path)


def context(variable, args):
    comparison_path = args.comparison_dir / f'{variable}_matched_monthly_means.nc'
    observation_path = args.comparison_dir / 'observations' / f'{variable}_selected_monthly_fields.nc'
    selected = json.loads((args.comparison_dir / 'selected_observations.json').read_text())[variable]
    with xr.open_dataset(comparison_path) as ds:
        keys = list(map(int, time_axis(ds)))
        mask = ds.comparison_mask.values.astype(bool)
        product = ds.attrs['observation']
        coverage = float(ds.attrs['land_coverage_fraction'])
        old_groups = {str(name): str(group) for name, group in zip(ds.series.values, ds.group.values) if str(name) != 'Observation'}
        previous = {str(name): ds.global_monthly_mean.sel(series=name).values.copy()
                    for name in ds.series.values}
    rows = inventory(args.root, variable, None)
    if {name: group for group, name, _ in rows} != old_groups:
        raise ValueError(f'{variable}: available models differ from the matched comparison; rerun monthly_mean_boxplots.py first')
    if selected['product'] != product:
        raise ValueError(f'{variable}: selected reference differs from the matched comparison')
    with xr.open_dataset(observation_path) as obs:
        if obs.attrs.get('product') != product:
            raise ValueError(f'{variable}: selected observation maps are stale')
        obs_keys = list(map(int, time_axis(obs)))
        lookup = {k: i for i, k in enumerate(obs_keys)}
        if not set(keys).issubset(lookup): raise ValueError(f'{variable}: observation months are missing')
        observation = obs.field.isel(time=[lookup[k] for k in keys]).values[:, mask]
    if not mask.any() or not np.isfinite(observation).all():
        raise ValueError(f'{variable}: invalid data on the existing fixed land domain')
    observed_means = np.average(observation, axis=1, weights=AREA[mask])
    np.testing.assert_allclose(observed_means, previous['Observation'], rtol=1e-12, atol=1e-12,
                               err_msg=f'{variable}: observation maps no longer reproduce the previous monthly means')
    reference_durations, reference_calendar = month_durations(Path(selected['source']), keys)
    reference_climo, reference_weight_days = weighted_climatologies(observation, keys, reference_durations)
    stat = observation_path.stat()
    raw_stat = Path(selected['source']).stat()
    signature = hashlib.sha256(json.dumps([str(observation_path.resolve()), stat.st_size, stat.st_mtime_ns,
        variable, keys, hashlib.sha256(mask.tobytes()).hexdigest(), raw_stat.st_size, raw_stat.st_mtime_ns, reference_durations.tolist(), 'duration-climatology-v2']).encode()).hexdigest()
    array_path = args.cache_dir / (signature + '_observation.npy')
    if not array_path.exists():
        tmp = array_path.with_name(array_path.name + f'.{os.getpid()}.tmp')
        with tmp.open('wb') as handle: np.save(handle, observation)
        tmp.replace(array_path)
    reference_climo_path = args.cache_dir / (signature + '_climatology.npy')
    temporary_climo = reference_climo_path.with_name(reference_climo_path.name + f'.{os.getpid()}.tmp')
    with temporary_climo.open('wb') as handle: np.save(handle, reference_climo)
    temporary_climo.replace(reference_climo_path)
    if args.limit:
        rows = [r for group in [g for g, _ in GROUPS] + ['CMIP6'] for r in [x for x in rows if x[0] == group][:args.limit]]
    return rows, keys, mask, product, coverage, signature, array_path, previous, reference_climo_path, reference_climo, reference_weight_days, reference_calendar


def compute_variable(variable, args):
    rows, keys, mask, product, coverage, signature, observed_path, previous, reference_climo_path, reference_climo, reference_weight_days, reference_calendar = context(variable, args)
    print(f'{variable}: {len(rows)} members/models; {len(keys)} matched months; shared land {coverage:.1%}', flush=True)
    tasks = [(path, variable, keys, mask, observed_path, signature, args.cache_dir, reference_climo_path) for _, _, path in rows]
    records, rmse_maps, bias_maps, monthly_rmse, monthly_global_bias = [], [], [], [], []
    climatology_bias_maps, model_weights, model_calendars = [], [], []
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        for (group, member, path), result in zip(rows, pool.map(model_metrics, tasks)):
            if not np.array_equal(result['keys'], keys): raise ValueError(f'{member}: month order mismatch')
            # The new local-error calculation must independently reproduce the
            # previous global-mean metric after averaging each month's errors.
            expected = previous[member] - previous['Observation']
            np.testing.assert_allclose(result['monthly_global_bias'], expected, rtol=1e-10, atol=1e-10,
                                       err_msg=f'{member}: source data no longer match the earlier comparison')
            record = dict(variable=variable, group=group, member=member, units=UNITS[variable], observation=product,
                          first_month=f'{keys[0] // 12:04d}-{keys[0] % 12 + 1:02d}',
                          last_month=f'{keys[-1] // 12:04d}-{keys[-1] % 12 + 1:02d}', months=len(keys),
                          land_coverage_fraction=coverage, source=str(path))
            for name in list(METRICS) + ['pooled_space_time', 'global_monthly_mean_rmse', 'area_mean_bias']:
                record[name] = float(result[name])
            record['model_calendar'] = str(result['calendar'])
            record['reference_calendar'] = reference_calendar
            records.append(record)
            climatology_bias_maps.append(result['climatology_bias'])
            model_weights.append(result['climatology_model_weight_days'])
            model_calendars.append(str(result['calendar']))
            rmse_maps.append(result['cell_rmse']); bias_maps.append(result['cell_bias'])
            monthly_rmse.append(result['monthly_spatial_rmse']); monthly_global_bias.append(result['monthly_global_bias'])
            print(f'  {group}: {member}; ANN climatological spatial RMSE={record["climatology_ANN"]:.6g}', flush=True)
    full_rmse = np.full((len(rows), 180, 360), np.nan)
    full_bias = np.full_like(full_rmse, np.nan)
    full_rmse[:, mask] = np.asarray(rmse_maps); full_bias[:, mask] = np.asarray(bias_maps)
    full_climatology_bias = np.full((len(rows), len(SEASONS), 180, 360), np.nan)
    full_climatology_bias[:, :, mask] = np.asarray(climatology_bias_maps)
    full_reference_climo = np.full((len(SEASONS), 180, 360), np.nan)
    full_reference_climo[:, mask] = reference_climo
    ds = xr.Dataset({
        'climatology_bias': (('member', 'season', 'lat', 'lon'), full_climatology_bias, {'units': UNITS[variable], 'definition': 'duration-weighted model climatology minus duration-weighted reference climatology'}),
        'reference_climatology': (('season', 'lat', 'lon'), full_reference_climo, {'units': UNITS[variable]}),
        'model_climatology_weight_days': (('member', 'season'), np.asarray(model_weights), {'units': 'days'}),
        'reference_climatology_weight_days': ('season', reference_weight_days, {'units': 'days'}),
        'model_calendar': ('member', model_calendars),
        'cell_temporal_rmse': (('member', 'lat', 'lon'), full_rmse, {'units': UNITS[variable], 'definition': 'sqrt(mean_time((model-observation)^2))'}),
        'cell_mean_bias': (('member', 'lat', 'lon'), full_bias, {'units': UNITS[variable]}),
        'monthly_spatial_rmse': (('member', 'time'), np.asarray(monthly_rmse), {'units': UNITS[variable], 'definition': 'sqrt(area_weighted_mean_space((model-observation)^2))'}),
        'monthly_global_mean_bias': (('member', 'time'), np.asarray(monthly_global_bias), {'units': UNITS[variable]}),
        'comparison_mask': (('lat', 'lon'), mask.astype('int8')),
        'cell_area': (('lat', 'lon'), AREA, {'units': 'km2'}),
        'group': ('member', [r[0] for r in rows]),
    }, coords={'member': [r[1] for r in rows], 'lat': LAT, 'lon': LON, 'time': dates(keys), 'season': list(SEASONS)}, attrs={
        'observation': product, 'variable': variable, 'land_coverage_fraction': coverage,
        'comparison_file': str(args.comparison_dir / f'{variable}_matched_monthly_means.nc'),
        'temporal_weighting': 'Climatologies use interval-duration weights in each input calendar. Additional temporal-error diagnostics use equal month weights',
        'reference_calendar': reference_calendar,
        'upstream_reference_commit': 'e209fa49cadc2a4e589f737d928b7aaf4298f703',
        'default_metric': 'climatology_ANN',
        'climatological_spatial_rmse_definition': 'sqrt(area_mean((duration_weighted_model_climatology - duration_weighted_reference_climatology)^2))',
        'spatial_domain': 'Identical fixed mask to the previous monthly global-mean comparison; all included models checked against its manifest',
        'cellwise_temporal_definition': 'area_weighted_mean_space(sqrt(mean_time(error^2)))',
        'monthly_spatial_definition': 'mean_time(sqrt(area_weighted_mean_space(error^2)))',
        'period_mean_spatial_definition': 'sqrt(area_weighted_mean_space(mean_time(error)^2))',
        'pooled_space_time_definition': 'sqrt(area_weighted_mean_space(mean_time(error^2)))',
        'observation_context_signature': signature,
    })
    for metric in list(METRICS) + ['pooled_space_time', 'global_monthly_mean_rmse', 'area_mean_bias']:
        ds[metric] = ('member', [r[metric] for r in records], {'units': UNITS[variable]})
    path = args.output_dir / f'{variable}_spatial_rmse_fields.nc'
    tmp = path.with_name(path.stem + f'.{os.getpid()}.tmp.nc')
    ds.to_netcdf(tmp, encoding={v: {'zlib': True, 'complevel': 1} for v in ['cell_temporal_rmse', 'cell_mean_bias', 'climatology_bias', 'reference_climatology']})
    tmp.replace(path)
    atomic_csv(args.output_dir / f'spatial_rmse_{variable}.csv', records)
    print(f'Saved {variable} spatial RMSE fields and metrics.', flush=True)


def combine(args):
    records = []
    for variable in TITLES:
        path = args.output_dir / f'spatial_rmse_{variable}.csv'
        if not path.exists(): return None
        with path.open() as handle: records.extend(csv.DictReader(handle))
    atomic_csv(args.output_dir / 'spatial_rmse_metrics.csv', records)
    # Portable plotting table: retain every individual value, including outliers.
    columns = ['variable', 'group', 'member', 'units', 'observation',
               'first_month', 'last_month', 'months', 'land_coverage_fraction']
    plotting_rows = []
    for row in records:
        compact = {key: row[key] for key in columns}
        compact['plot_label'] = {'lai': 'LAI', 'et': 'ET', 'mrsos': 'SM'}[row['variable']]
        compact.update({f'rmse_{season}': row[f'climatology_{season}'] for season in SEASONS})
        plotting_rows.append(compact)
    atomic_csv(args.output_dir / 'spatial_rmse_plot_data.csv', plotting_rows)
    return records


def plot(records, args):
    fig, axes = plt.subplots(1, 3, figsize=(20, 6.2))
    outliers = []
    labels = [g for g, _ in GROUPS] + ['CMIP6']
    handles = [Patch(facecolor=c, alpha=.7, label='E3SM ' + g) for c, (g, _) in zip(COLORS, GROUPS)]
    handles += [Patch(facecolor='white', edgecolor='.3', label='CMIP6')]
    handles += [Line2D([], [], marker='o', linestyle='', color=c, markeredgecolor='black', label='Mean ' + g) for c, (g, _) in zip(COLORS, GROUPS)]
    for ax, variable in zip(axes, TITLES):
        subset = [r for r in records if r['variable'] == variable]
        right = ax.twinx()
        for i, group in enumerate(labels):
            values = [float(r[args.metric]) for r in subset if r['group'] == group]
            if not values: raise ValueError(f'{variable}: empty group {group}')
            q1, q3 = np.percentile(values, [25, 75])
            lower, upper = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
            if args.whiskers == 'range': lower, upper = min(values), max(values)
            for row in (r for r in subset if r['group'] == group):
                value = float(row[args.metric])
                if value < lower or value > upper:
                    outliers.append({**row, 'plotted_metric': args.metric, 'plotted_value': value,
                                     'direction': 'low' if value < lower else 'high',
                                     'lower_fence': lower, 'upper_fence': upper})
            target = ax if i < 3 else right
            box = target.boxplot([values], positions=[i + 1], widths=.5, patch_artist=True,
                whis=[0, 100] if args.whiskers == 'range' else 1.5,
                boxprops={'edgecolor': '.3', 'linewidth': 1.4}, medianprops={'color': '#ff7f0e', 'linewidth': 1.7},
                flierprops={'marker': 'o', 'markerfacecolor': 'none', 'markersize': 6})
            box['boxes'][0].set_facecolor(COLORS[i] if i < 3 else 'white')
            box['boxes'][0].set_alpha(.7 if i < 3 else 1)
            if i < 3: right.scatter(3.9 + .1 * i, np.mean(values), color=COLORS[i], edgecolors='black', s=55, zorder=5)
        panel_titles = {'lai': 'LAI (m²/m²)', 'et': 'ET (mm/day)', 'mrsos': 'SM (kg/m²)'}
        ax.set_title(panel_titles[variable], fontsize=18, fontweight='bold', pad=12)
        ax.set_ylabel('RMSE(E3SM configs)', fontsize=14)
        right.set_ylabel('RMSE(CMIP6)', fontsize=14)
        ax.set_xticks([1, 2, 3, 4], labels, rotation=16); ax.set_xlim(.5, 4.5)
        ax.tick_params(axis='both', labelsize=13)
        right.tick_params(axis='y', labelsize=13)
        ax.axvline(3.5, color='.7', linestyle='--', lw=.8); ax.grid(axis='y', linestyle='--', alpha=.4)
        ax.set_ylim(bottom=max(0, ax.get_ylim()[0])); right.set_ylim(bottom=max(0, right.get_ylim()[0]))
    fig.suptitle(METRICS[args.metric], fontsize=17, fontweight='bold', y=.99)
    fig.legend(handles=handles, loc='lower center', ncol=4, fontsize=11)
    fig.subplots_adjust(left=.06, right=.94, bottom=.25, top=.79, wspace=.65)
    whisker_suffix = '_range' if args.whiskers == 'range' else ''
    for ext in ['png', 'pdf']:
        fig.savefig(args.output_dir / f'spatial_rmse_boxplots_{args.metric}{whisker_suffix}.{ext}', dpi=220, bbox_inches='tight')
    plt.close(fig)
    outlier_path = args.output_dir / f'spatial_rmse_outliers_{args.metric}{whisker_suffix}.csv'
    if outliers:
        atomic_csv(outlier_path, outliers)
    else:
        with outlier_path.open('w', newline='') as handle:
            csv.writer(handle).writerow(list(records[0]) + ['plotted_metric', 'plotted_value', 'direction', 'lower_fence', 'upper_fence'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--comparison-dir', type=Path, default=HERE / 'output')
    parser.add_argument('--output-dir', type=Path, default=HERE / 'output/spatial_rmse')
    parser.add_argument('--cache-dir', type=Path, default=HERE / '.cache_spatial')
    parser.add_argument('--metric', choices=list(METRICS), default='climatology_ANN')
    parser.add_argument('--whiskers', choices=['iqr', 'range'], default='iqr', help='IQR matches existing plots; range matches the reference script min/max whiskers')
    parser.add_argument('--season', choices=list(SEASONS), help='Select E3SM Diagnostics-style ANN/DJF/MAM/JJA/SON climatological spatial RMSE')
    parser.add_argument('--variable', choices=list(TITLES))
    parser.add_argument('--jobs', type=int, default=3)
    parser.add_argument('--limit', type=int, help='Check N models per group on the full comparison mask; save under output-dir/smoke')
    parser.add_argument('--plot-only', action='store_true')
    args = parser.parse_args()
    if args.season: args.metric = f'climatology_{args.season}'
    if args.jobs < 1 or (args.limit is not None and args.limit < 1): parser.error('jobs and limit must be positive')
    if args.limit: args.output_dir = args.output_dir / 'smoke'
    args.output_dir.mkdir(parents=True, exist_ok=True); args.cache_dir.mkdir(parents=True, exist_ok=True)
    if not args.plot_only:
        for variable in ([args.variable] if args.variable else TITLES): compute_variable(variable, args)
    records = combine(args)
    if records is None:
        print('Variable checkpoint saved; compute the remaining variables to draw the combined boxplots.', flush=True)
    else:
        plot(records, args)
        print(f'Saved {args.metric} boxplots in {args.output_dir}', flush=True)


if __name__ == '__main__': main()
