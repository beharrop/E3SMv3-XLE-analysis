"""E3SM Diagnostics-style climatological spatial RMSE and local-error diagnostics.

Climatologies use each input interval duration; additional temporal-error
diagnostics use equal month weights. All accumulations use float64.
"""
import hashlib
import json
import os
from pathlib import Path
import numpy as np
import xarray as xr
from land_common import monthly_blocks

SEASONS = {'ANN': tuple(range(1, 13)), 'DJF': (12, 1, 2), 'MAM': (3, 4, 5), 'JJA': (6, 7, 8), 'SON': (9, 10, 11)}

METRICS = {
    **{f'climatology_{season}': f'E3SM Diagnostics-style {season} climatological spatial RMSE' for season in SEASONS},
    'cellwise_temporal': 'Area mean of grid-cell temporal RMSE',
    'monthly_spatial': 'Time mean of monthly spatial RMSE',
    'period_mean_spatial': 'Spatial RMSE of period-mean maps',
}


def month_durations(path, keys):
    """Return interval lengths in days, using each input's own calendar.

    Match intervals by their midpoints (ELM timestamps may label the next month).
    This follows the time-bound weighting in E3SM Diagnostics climo_xr.climo.
    """
    from land_common import time_axis
    with xr.open_dataset(path) as ds:
        axis = time_axis(ds)
        name = ds.time.attrs.get('bounds')
        if name not in ds:
            name = next((n for n in ['time_bounds', 'time_bnds'] if n in ds), None)
        if name:
            intervals = ds[name].values
            days = np.array([float((hi - lo) / np.timedelta64(1, 'D'))
                if isinstance(hi - lo, np.timedelta64) else (hi - lo).total_seconds() / 86400.
                for lo, hi in intervals])
        else:
            days = np.asarray(ds.time.dt.days_in_month, dtype=float)
        calendar = ds.time.encoding.get('calendar', getattr(ds.time.values[0], 'calendar', 'proleptic_gregorian'))
    lookup = {int(k): float(day) for k, day in zip(axis, days)}
    if not set(keys).issubset(lookup): raise ValueError(f'{path}: missing time weights')
    result = np.asarray([lookup[k] for k in keys])
    if not np.isfinite(result).all() or np.any(result <= 0): raise ValueError(f'{path}: invalid interval lengths')
    return result, str(calendar)


def weighted_climatologies(fields, keys, durations):
    months = np.asarray(keys) % 12 + 1
    climatologies, totals = [], []
    for season, members in SEASONS.items():
        selected = np.isin(months, members)
        if not selected.any(): raise ValueError(f'No months for {season}')
        climatologies.append(np.average(np.asarray(fields)[selected], axis=0, weights=durations[selected]))
        totals.append(durations[selected].sum())
    return np.asarray(climatologies), np.asarray(totals)


class ErrorAccumulator:
    def __init__(self, weights):
        self.weights = np.asarray(weights, dtype=np.float64)
        if self.weights.ndim != 1 or not np.all(np.isfinite(self.weights)) or np.any(self.weights <= 0):
            raise ValueError('Positive finite cell areas are required')
        self.sse = np.zeros(len(weights), dtype=np.float64)
        self.error_sum = np.zeros_like(self.sse)
        self.monthly_spatial_rmse = []
        self.monthly_global_bias = []
        self.count = 0

    def update(self, errors):
        errors = np.asarray(errors, dtype=np.float64)
        if errors.ndim != 2 or errors.shape[1] != len(self.weights) or not np.isfinite(errors).all():
            raise ValueError('Errors must have shape (month, cell), with no missing values')
        self.sse += np.sum(errors ** 2, axis=0)
        self.error_sum += np.sum(errors, axis=0)
        self.monthly_spatial_rmse.extend(np.sqrt(np.average(errors ** 2, weights=self.weights, axis=1)))
        self.monthly_global_bias.extend(np.average(errors, weights=self.weights, axis=1))
        self.count += len(errors)

    def finish(self):
        if not self.count: raise ValueError('No monthly errors accumulated')
        cell_rmse = np.sqrt(self.sse / self.count)
        cell_bias = self.error_sum / self.count
        return dict(
            cell_rmse=cell_rmse, cell_bias=cell_bias,
            monthly_spatial_rmse=np.asarray(self.monthly_spatial_rmse),
            monthly_global_bias=np.asarray(self.monthly_global_bias),
            cellwise_temporal=float(np.average(cell_rmse, weights=self.weights)),
            monthly_spatial=float(np.mean(self.monthly_spatial_rmse)),
            period_mean_spatial=float(np.sqrt(np.average(cell_bias ** 2, weights=self.weights))),
            pooled_space_time=float(np.sqrt(np.average(self.sse / self.count, weights=self.weights))),
            global_monthly_mean_rmse=float(np.sqrt(np.mean(np.asarray(self.monthly_global_bias) ** 2))),
            area_mean_bias=float(np.average(cell_bias, weights=self.weights)),
        )


def atomic_npz(path, data):
    tmp = path.with_name(path.name + f'.{os.getpid()}.tmp')
    with tmp.open('wb') as handle: np.savez_compressed(handle, **data)
    tmp.replace(path)


def model_metrics(task):
    path, variable, keys, mask, observation_array, context_signature, cache_dir, reference_climo_array = task
    path = Path(path)
    before = path.stat()
    signature = [str(path.resolve()), before.st_size, before.st_mtime_ns,
                 context_signature, 'spatial-errors-climatology-v2']
    # Include the implementation fingerprints so changed grid/error methods
    # cannot silently reuse a result from an earlier implementation.
    for script in [Path(__file__), Path(__file__).with_name('land_common.py')]:
        signature.append(hashlib.sha256(script.read_bytes()).hexdigest())
    cache = Path(cache_dir) / (hashlib.sha256(json.dumps(signature).encode()).hexdigest() + '.npz')
    if cache.exists():
        with np.load(cache) as data:
            return {name: data[name].copy() for name in data.files}
    observed = np.load(observation_array, mmap_mode='r')
    from land_common import AREA
    accumulator = ErrorAccumulator(AREA[mask])
    durations, calendar = month_durations(path, keys)
    climatology_sum = np.zeros((len(SEASONS), int(mask.sum())), dtype=np.float64)
    duration_sum = np.zeros(len(SEASONS), dtype=np.float64)
    lookup = {int(k): i for i, k in enumerate(keys)}
    seen = set()
    axes = []
    for axis, fields in monthly_blocks(path, variable, keys):
        indices = [lookup[int(k)] for k in axis]
        modeled = fields[:, mask].astype(np.float64)
        errors = modeled - observed[indices].astype(np.float64)
        accumulator.update(errors)
        months = np.asarray(axis) % 12 + 1
        for j, season_months in enumerate(SEASONS.values()):
            selected = np.isin(months, season_months)
            if selected.any():
                weight = durations[np.asarray(indices)[selected]]
                climatology_sum[j] += np.sum(modeled[selected] * weight[:, None], axis=0)
                duration_sum[j] += weight.sum()
        axes.extend(map(int, axis)); seen.update(map(int, axis))
    if seen != set(keys) or len(axes) != len(keys):
        raise ValueError(f'{path}: unmatched or duplicate monthly records')
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f'{path}: input changed during calculation')
    result = accumulator.finish()
    if np.any(duration_sum <= 0): raise ValueError(f'{path}: incomplete seasonal climatologies')
    reference_climatology = np.load(reference_climo_array, mmap_mode='r')
    climatology_bias = climatology_sum / duration_sum[:, None] - reference_climatology
    result['climatology_bias'] = climatology_bias
    result['climatology_model_weight_days'] = duration_sum
    result['calendar'] = np.asarray(calendar)
    for j, season in enumerate(SEASONS):
        result[f'climatology_{season}'] = float(np.sqrt(np.average(climatology_bias[j] ** 2, weights=AREA[mask])))
    order = np.argsort(axes)
    result['monthly_spatial_rmse'] = result['monthly_spatial_rmse'][order]
    result['monthly_global_bias'] = result['monthly_global_bias'][order]
    result['keys'] = np.asarray(axes)[order]
    atomic_npz(cache, result)
    return result
