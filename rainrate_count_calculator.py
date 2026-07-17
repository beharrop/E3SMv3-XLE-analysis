#!/bin/python

import os
import numpy as np
import xarray as xr
import scipy.stats as stats
import argparse

def convert_PRECT_units(PRECT, desired_units):
    if(PRECT.attrs['units'].lower()=='mm/day' and desired_units.lower()=='kg/m2/s'):
        PRECT = PRECT / 86400.
    elif(PRECT.attrs['units'].lower()=='kg/m2/s' and desired_units.lower()=='mm/day'):
        PRECT = PRECT * 86400.
    elif(PRECT.attrs['units'].lower()=='m/s' and desired_units.lower()=='kg/m2/s'):
        PRECT = PRECT * 1e3
    elif(PRECT.attrs['units'].lower()=='m/s' and desired_units.lower()=='mm/day'):
        PRECT = PRECT * 86400e3
    PRECT.attrs.update({'units': desired_units})
    return PRECT

def use_month(month, month_mask=np.arange(1,13)):
    return np.isin(month, month_mask)


def compute_rainrate_counts(
    in_file,
    out_file,
    smallest_bin_center=0.03,
    growth_rate=0.07,
    precip_name='PRECT',
    start_time=None,
    end_time=None,
    month_mask=None,
):
    c1 = smallest_bin_center

    if month_mask is None:
        month_mask = np.arange(1, 13)
        print('Using all times')
    else:
        print('Using only months with indexes in', month_mask)

    fac   = 1. + growth_rate
    x1    = np.sqrt(c1**2/fac)                # center to edge factor
    cntrs = np.append([0.], c1*fac**np.arange(0,151))
    edges = np.append([0.], x1*fac**np.arange(0,152))

    with xr.open_dataset(in_file) as data:
        try:
            data = data.sel(time=slice(start_time, end_time))
        except KeyError:
            print("Time slicing only valid if 'time' is a dimension within the dataset")
        data   = data.load()
        precip = convert_PRECT_units(data[precip_name], desired_units='mm/day').compute()
        ds     = xr.Dataset()

        if ('lon' in data.dims) and ('lat' in data.dims):
            counts     = np.zeros([len(cntrs), len(data['lon']), len(data['lat'])])
            rel_counts = np.zeros([len(cntrs), len(data['lon']), len(data['lat'])])
            for m, lon in enumerate(data['lon']):
                for n, lat in enumerate(data['lat']):
                    prec_ts           = precip.sel(lon=lon, lat=lat, 
                                                   time=use_month(precip['time.month'], month_mask))
                    stat_metric       = stats.binned_statistic(prec_ts, prec_ts, statistic='count', bins=edges)
                    counts[:,m,n]     = stat_metric.statistic
                    rel_counts[:,m,n] = stat_metric.statistic / sum(stat_metric.statistic)
            ds['count']     = xr.DataArray(counts, 
                                           coords={'centers':cntrs, 'lon':data['lon'], 'lat':data['lat']},
                                           dims=('centers', 'lon', 'lat'))
            ds['rel_count'] = xr.DataArray(rel_counts, 
                                           coords={'centers':cntrs, 'lon':data['lon'], 'lat':data['lat']},
                                           dims=('centers', 'lon', 'lat'))

        if ('ncol' in data.dims):
            counts     = np.zeros([len(cntrs), len(data['ncol'])])
            rel_counts = np.zeros([len(cntrs), len(data['ncol'])])
            for n, ncol in enumerate(data['ncol']):
                prec_ts         = precip.sel(ncol=ncol,
                                             time=use_month(precip['time.month'], month_mask))
                stat_metric     = stats.binned_statistic(prec_ts, prec_ts, statistic='count', bins=edges)
                counts[:,n]     = stat_metric.statistic
                rel_counts[:,n] = stat_metric.statistic / sum(stat_metric.statistic)
            ds['count']     = xr.DataArray(counts, 
                                           coords={'centers':cntrs, 'ncol':data['ncol']},
                                           dims=('centers', 'ncol'))
            ds['rel_count'] = xr.DataArray(rel_counts, 
                                           coords={'centers':cntrs, 'ncol':data['ncol']},
                                           dims=('centers', 'ncol'))

        ds.coords['edges'] = ('edges', edges)
        ds.attrs.update({'Smallest bin center': c1,
                         'growth factor percentage': fac,
                         'center to edge factor': x1,
                         'author': 'Bryce Harrop'})
        ds.to_netcdf(out_file, mode='w')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Provide precipitation file and pdf details')
    parser.add_argument('--in_file', type=str,
                        help='a string specifying the precipitation file')
    parser.add_argument('--out_file', type=str,
                        help='a string specifying the amount distribution file')
    parser.add_argument('--smallest_bin_center', type=float, default=0.03,
                        help='a float specifying the smallest bin center of the pdf')
    parser.add_argument('--growth_rate', type=float, default=0.07,
                        help='a float specifying the growth rate of the pdf')
    parser.add_argument('--precip_name', type=str, default='PRECT',
                        help='a string specifying the variable name for precipitation')
    parser.add_argument('--start_time', type=str, default=None,
                        help='specify the desired start time within the precip file')
    parser.add_argument('--end_time',   type=str, default=None,
                        help='specify the desired end time within the precip file')
    parser.add_argument('--month_mask', type=int, nargs='+', default=None, 
                        help='specify which months to include (as list)')
    args = parser.parse_args()

    compute_rainrate_counts(
        in_file=args.in_file,
        out_file=args.out_file,
        smallest_bin_center=args.smallest_bin_center,
        growth_rate=args.growth_rate,
        precip_name=args.precip_name,
        start_time=args.start_time,
        end_time=args.end_time,
        month_mask=args.month_mask,
    )

