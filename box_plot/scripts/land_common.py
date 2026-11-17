"""Shared monthly time, unit, missing-data and 1-degree grid handling."""
from pathlib import Path
import numpy as np
import xarray as xr

DATA = Path('/global/cfs/cdirs/e3sm/feng809/data/ILAMB-DATA')
LAT = np.arange(-89.5, 90., 1.)
LON = np.arange(-179.5, 180., 1.)
AREA = np.broadcast_to((np.sin(np.deg2rad(LAT + .5)) - np.sin(np.deg2rad(LAT - .5)))[:, None]
                       * np.deg2rad(1.) * 6371.**2, (180, 360)).copy()
UNITS = {'lai': 'm2 m-2', 'et': 'mm day-1', 'mrsos': 'kg m-2'}
NAMES = {'lai': ['lai', 'TLAI'], 'et': ['et', 'evspsbl', 'hfls'], 'mrsos': ['mrsos', 'SOILWATER_10CM']}


def land_mask(data_root=DATA):
    path = Path(data_root) / 'regions/GlobalLandNoAnt.nc'
    with xr.open_dataset(path) as ds:
        a = ds['ids'].load()
    a = a.assign_coords(lon=((a.lon + 180) % 360) - 180).sortby('lat').sortby('lon')
    a = a.reindex(lat=LAT, lon=LON)
    return np.isfinite(a.values)


def data_variable(ds, variable):
    for name in NAMES[variable]:
        if name in ds and set(ds[name].dims) == {'time', 'lat', 'lon'}:
            return ds[name].transpose('time', 'lat', 'lon')
    raise ValueError(f'Cannot identify {variable} in {list(ds.data_vars)}')


def time_axis(ds):
    bound = ds.time.attrs.get('bounds')
    if bound not in ds:
        bound = next((b for b in ['time_bounds', 'time_bnds'] if b in ds), None)
    if bound:
        b = ds[bound].values
        t = xr.DataArray([lo + (hi - lo) / 2 for lo, hi in b], dims='time')
    else:
        t = ds.time
    keys = np.asarray(t.dt.year * 12 + t.dt.month - 1, dtype=int)
    if len(set(keys)) != len(keys):
        raise ValueError('Multiple records in a calendar month; monthly input is required')
    return keys


def dates(keys):
    return np.array([f'{k // 12:04d}-{k % 12 + 1:02d}-01' for k in keys], dtype='datetime64[ns]')


def clean_and_convert(values, variable, units, latent_heat=2.45e6):
    a = np.asarray(values, dtype=np.float64)
    good = np.isfinite(a) & (np.abs(a) < 1e19)
    # Several ILAMB files omit missing-value metadata for these native sentinels.
    good &= ~np.isin(a, [-999., -9999., -10000., -99999.])
    a = np.where(good, a, np.nan)
    u = str(units).lower().replace(' ', '').replace('**', '').replace('^', '')
    if variable == 'lai' and u in ['1', '', 'm2m-2', 'm2/m2', 'dimensionless']:
        return np.where(a >= 0, a, np.nan)
    if variable == 'et':
        if u in ['kgm-2s-1', 'kg/m2/s', 'mms-1', 'mm/s']:
            return a * 86400
        if u in ['mmday-1', 'mmd-1', 'mm/day', 'mm/d']:
            return a
        if u in ['wm-2', 'w/m2']:
            return a * 86400 / latent_heat
    if variable == 'mrsos' and u in ['kgm-2', 'kg/m2', 'mm']:
        return np.where(a >= 0, a, np.nan)
    raise ValueError(f'Unsupported units for {variable}: {units!r}')


class Grid:
    """Area-average 0.5-degree grids; preserve 1-degree grids; map coarse cells.

    Coarse products retain their native cell values over the cell footprint.
    No linear coastal interpolation or extrapolation is used.
    """
    def __init__(self, lat, lon):
        lat, lon = np.asarray(lat), (np.asarray(lon) + 180) % 360 - 180
        self.ilat, self.ilon = np.argsort(lat), np.argsort(lon)
        self.lat, self.lon = lat[self.ilat], lon[self.ilon]
        if len(np.unique(self.lon)) != len(self.lon):
            raise ValueError('Repeated longitude coordinates')
        if np.allclose(np.diff(self.lat), .5) and np.allclose(np.diff(self.lon), .5) and len(lat) == 360 and len(lon) == 720:
            if not np.allclose(self.lat, np.arange(-89.75, 90, .5)) or not np.allclose(self.lon, np.arange(-179.75, 180, .5)):
                raise ValueError('Unexpected 0.5-degree grid alignment')
            self.kind = 'half'
        elif len(lat) == 180 and len(lon) == 360 and np.allclose(self.lat, LAT) and np.allclose(self.lon, LON):
            self.kind = 'one'
        else:
            self.kind = 'coarse'
            if np.min(np.diff(self.lat)) < 1 or np.min(np.diff(self.lon)) < 1:
                raise ValueError('Unsupported fine grid; provide regular 0.5 or 1 degree input')
            self.y, self.valid_y = self.cell_index(self.lat, LAT)
            self.x, self.valid_x = self.cell_index(self.lon, LON)

    @staticmethod
    def cell_index(source, target):
        edges = np.r_[source[0] - (source[1] - source[0]) / 2,
                      (source[1:] + source[:-1]) / 2,
                      source[-1] + (source[-1] - source[-2]) / 2]
        index = np.searchsorted(edges, target, side='right') - 1
        valid = (index >= 0) & (index < len(source))
        return np.clip(index, 0, len(source) - 1), valid

    def apply(self, values):
        a = values[:, self.ilat][:, :, self.ilon]
        if self.kind == 'half':
            a = a.reshape(len(a), 180, 2, 360, 2)
            w = np.cos(np.deg2rad(self.lat)).reshape(1, 180, 2, 1, 1)
            good = np.isfinite(a)
            denominator = np.sum(good * w, axis=(2, 4))
            numerator = np.sum(np.where(good, a, 0) * w, axis=(2, 4))
            return np.divide(numerator, denominator, out=np.full_like(numerator, np.nan), where=denominator > 0)
        if self.kind == 'one':
            return a
        result = a[:, self.y][:, :, self.x]
        return np.where(self.valid_y[None, :, None] & self.valid_x[None, None, :], result, np.nan)


def monthly_blocks(path, variable, keys=None, block=24):
    with xr.open_dataset(path) as ds:
        a = data_variable(ds, variable)
        axis = time_axis(ds)
        index = np.arange(len(axis)) if keys is None else np.flatnonzero(np.isin(axis, list(keys)))
        grid = Grid(a.lat.values, a.lon.values)
        for offset in range(0, len(index), block):
            idx = index[offset:offset + block]
            values = clean_and_convert(a.isel(time=idx).values, variable, a.attrs.get('units', ''))
            yield axis[idx], grid.apply(values).astype(np.float32)


def weighted_means(fields, mask):
    good = np.isfinite(fields) & mask
    weights = good * AREA
    numerator = np.sum(np.where(good, fields, 0) * AREA, axis=(-2, -1))
    denominator = np.sum(weights, axis=(-2, -1))
    means = np.divide(numerator, denominator, out=np.full_like(numerator, np.nan), where=denominator > 0)
    return means, denominator


def file_for(directory, variable):
    prefixes = ['et', 'evspsbl'] if variable == 'et' else NAMES[variable]
    for prefix in prefixes:
        files = sorted(Path(directory).glob(prefix + '_*.nc'))
        if len(files) > 1:
            raise ValueError(f'Ambiguous files in {directory}: {files}')
        if files:
            return files[0]
    return None
