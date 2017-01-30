#!/usr/bin/env python
"""
plotting swe percentiles
usage: <python> <swe_plot.py> <configuration.cfg>

Reads in a netcdf file converted from VIC fluxes.
Extracts one day's data based on date in config file.
Reads in saved cdfs.
Interpolates every value's  percentile 
based on where it falls relative to historic range. 
Save as new netcdf file to be read by plotting script.
"""
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import xarray as xr
import os
import time
import math
import gc
import argparse
from scipy.interpolate import interp1d
from netCDF4 import Dataset
from tonic.io import read_config

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file',
                    type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in from configuration file
direc = config_dict['VIC2NC']['OutputDirNC']
N = int(config_dict['ECFLOW']['Met_Delay'])

# number of plotting positions
num_pp = int(config_dict['PLOT']['num_plot_pos'])
cdf_loc = config_dict['PLOT']['cdf_SWE']
outfile = config_dict['PLOT']['Percent_SWE']

# how many days behind metdata is from realtime
date_unformat = datetime.now() - timedelta(days=N)

date = date_unformat.strftime('%Y-%m-%d')
date_ncfile = date_unformat.strftime('%Y%m%d')
month_day = date_unformat.strftime("%B_%-d")

# read VIC output that has been converted to a netcdf file
f = "pnw_daily_run.%s-%s.nc" % (date_ncfile, date_ncfile)

ds = xr.open_dataset(os.path.join(direc, f))

# create list of all latitudes and longitudes for full rectangle
un_lat = ds['lat'].values
un_lon = ds['lon'].values

num_lat = len(un_lat)
num_lon = len(un_lon)

latitude = np.repeat(un_lat, num_lon)
longitude = np.tile(un_lon, num_lat)

# Weibull Plotting Position
q = np.zeros(num_pp)
for i in range(1, num_pp + 1):
    q[i - 1] = (i / (num_pp + 1.0))

# create a min and max plotting position
# for any values that fall outside of historic range
min_q = min(q) / 2
max_q = max(q) + min_q

# select out time and variable data
ds_day = ds.sel(time=date)
swe_ds = ds_day['SWE']

# create a dictionary containing the lat, lon and corresponding percentile
# value (if one exists)
d = []

for lat, lon in zip(latitude, longitude):

    # iterate through all latitudes and longitudes
    ds_lat = swe_ds.sel(lat=lat)
    ds_lon = ds_lat.sel(lon=lon)

    value = ds_lon.values

    # read in sorted list of cdf values
    # if cdf cannot be read then that lat lon is saved in the dictionary without a percentile
    # this is done to make creating the xarray dataset easier later on
    try:
        cdf_file = "%s_%s" % (lat, lon)
        cdf_path = os.path.join(cdf_loc, month_day, cdf_file)
        cdf = pd.read_csv(cdf_path, index_col=None,
                          delimiter=None, header=None)
        x = cdf[0]

        # 10mm threshold (this is based on current monitor)

        if (value < 10):
            combine = (lat, lon)
            d.append(combine)
        else:

            try:
                # interpolate percentile based on where value falls
                # relative to historic range
                f = interp1d(x, q)
                percentile = f(value)
                combine = (lat, lon, float(percentile))
                d.append(combine)

            # if interpolation fails then a value is assigned based on
            # whether it is higher or lower than the range
            except ValueError:
                if (value > max(x)):
                    percentile = (max_q)
                    combine = (lat, lon, percentile)
                    d.append(combine)
                else:
                    percentile = (min_q)
                    combine = (lat, lon, percentile)
                    d.append(combine)
    except IOError:
        percentile = value
        combine = (lat, lon)
        d.append(combine)

# Dictionary to DataFrame to Dataset
df = pd.DataFrame(d, columns=["Latitude", "Longitude", "Percentile"])
a = df['Percentile'].values
new = a.reshape(195, 245)

dsx = xr.Dataset({'Percentile': (['lat', 'lon'], new)},
                 coords={'lon': (['lon'], un_lon), 'lat': (['lat'], un_lat)})

# save to netcdf
dsx.to_netcdf(outfile, mode='w', format='NETCDF4')
