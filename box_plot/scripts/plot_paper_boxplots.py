#!/usr/bin/env python3
"""Recreate the manuscript LAI/ET/SM figures using only the bundled CSV.

Uses the original plotting function; no NetCDF/model/observation files are read.
"""
import argparse
import csv
from pathlib import Path

import numpy as np

from spatial_rmse_et_sm_v1_v2 import plot
from spatial_rmse_metrics import SEASONS

PACKAGE = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, default=PACKAGE / 'data/spatial_rmse_plot_data.csv')
    parser.add_argument('--output-dir', type=Path, default=PACKAGE / 'figures/reproduced')
    parser.add_argument('--season', choices=['all'] + list(SEASONS), default='ANN')
    parser.add_argument('--whiskers', choices=['iqr', 'range'], default='iqr')
    args = parser.parse_args()
    with args.csv.open(newline='') as handle:
        records = list(csv.DictReader(handle))
    if not records:
        raise ValueError('The plotting CSV is empty')
    identities = [(row['variable'], row['group'], row['member']) for row in records]
    if len(identities) != len(set(identities)):
        raise ValueError('Duplicate variable/configuration/member rows')
    for row in records:
        if row['variable'] == 'lai' and row['plot_role'] != 'box':
            raise ValueError('LAI must not contain v1/v2 benchmark markers')
        for season in SEASONS:
            value = float(row[f'rmse_{season}'])
            if not np.isfinite(value) or value < 0:
                raise ValueError(f'Invalid {season} RMSE: {row["member"]}')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for season in SEASONS if args.season == 'all' else [args.season]:
        plot(records, args, season)
    print(f'Recreated figures from {len(records)} CSV rows in {args.output_dir}')


if __name__ == '__main__':
    main()
