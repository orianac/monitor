#!/usr/bin/env python
'''
creating snow water equivalent (swe) percentiles
usage: <python> <swe_plot.py> <configuration.cfg>

Reads in a netcdf file converted from VIC fluxes.
Extracts one day's data based on date in config file.
Reads in saved cdfs.
Interpolates every value's percentile
based on where it falls relative to historic range.
Save as new netcdf file.
'''
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
from collections import OrderedDict

# read in configuration file
parser = argparse.ArgumentParser(description='Calculate SWE percentiles')
parser.add_argument(
    'config_file',
    metavar='config_file',
    help='the python configuration file, see template: /monitor/config/python_template.cfg')
args = parser.parse_args()
config_dict = read_config(args.config_file)

# read in from configuration file
direc = config_dict['VIC']['OutputDirRoot']
n_days = int(config_dict['ECFLOW']['Met_Delay'])

# number of plotting positions
num_pp = int(config_dict['PERCENTILES']['num_plot_pos'])
cdf_loc = config_dict['PERCENTILES']['cdf_SWE']
outfile_loc = config_dict['PERCENTILES']['Percentile_Loc']

vic_start_date = config_dict['VIC']['vic_start_date']

# how many days behind metdata is from realtime
date = config_dict['VIC']['vic_end_date']

date_unformat = datetime.strptime(date, '%Y-%m-%d')
month_day = date_unformat.strftime('%B_%-d')

# read VIC output
f = 'fluxes.%s.nc' % (vic_start_date)
ds = xr.open_dataset(os.path.join(direc, f))

# create list of all latitudes and longitudes for full rectangle
un_lat = ds['lat'].values
un_lon = ds['lon'].values

num_lat = len(un_lat)
num_lon = len(un_lon)

latitude = np.repeat(un_lat, num_lon)
longitude = np.tile(un_lon, num_lat)

# Weibull Plotting Position (Weibull, W. 1951)
# we multiply by 100 to get values in the range of 0-100
q = 100 * (np.arange(1, num_pp + 1) / (num_pp + 1.0))

# create a min and max plotting position
# for any values that fall outside of historic range
min_q = min(q) / 2
max_q = max(q) + min_q

# select out time and variable data
ds_day = ds.sel(time=date)
swe_ds = ds_day['OUT_SWE']

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
        cdf_file = '%s_%s' % (lat, lon)
        cdf_path = os.path.join(cdf_loc, month_day, cdf_file)
        cdf = pd.read_csv(cdf_path, index_col=None,
                          delimiter=None, header=None)
        x = cdf[0]

        # 10mm threshold (this is based on current monitor)

        if (value < 10) and (x.mean() < 10) or math.isnan(value):
            combine = (lat, lon, np.nan, np.nan)
            d.append(combine)

        else:

            try:
                # interpolate percentile based on where value falls
                # relative to historic range
                f = interp1d(x, q)
                percentile = f(value)

                if percentile < 2:
                    category = 0
                elif 2 <= percentile < 5:
                    category = 1
                elif 5 <= percentile < 10:
                    category = 2
                elif 10 <= percentile < 20:
                    category = 3
                elif 20 <= percentile < 30:
                    category = 4
                elif 30 <= percentile < 70:
                    category = 5
                elif 70 <= percentile < 80:
                    category = 6
                elif 80 <= percentile < 90:
                    category = 7
                elif 90 <= percentile < 95:
                    category = 8
                elif 95 <= percentile < 98:
                    category = 9
                elif percentile >= 98:
                    category = 10

                combine = (lat, lon, float(percentile), category)
                d.append(combine)

            except NameError:
                print value

            # if interpolation fails then a value is assigned based on
            # whether it is higher or lower than the range
            except ValueError:
                if (value > max(x)):
                    percentile = (max_q)
                    category = 10
                    combine = (lat, lon, percentile, category)
                    d.append(combine)
                else:
                    percentile = (min_q)
                    category = 0
                    combine = (lat, lon, percentile, category)
                    d.append(combine)
    except IOError:
        percentile = value
        combine = (lat, lon, -9999.0, -9999.0)
        d.append(combine)

# Dictionary to DataFrame to Dataset
df = pd.DataFrame(
    d, columns=['Latitude', 'Longitude', 'swepercentile', 'category'])
a = df['swepercentile'].values
new = a.reshape(num_lat, num_lon)

b = df['category'].values
newb = b.reshape(num_lat, num_lon)

dsx = xr.Dataset({'swepercentile': (['lat', 'lon'], new), 'category': (
    ['lat', 'lon'], newb)}, coords={'lon': (['lon'], un_lon), 'lat': (['lat'], un_lat)})

dsx_attrs = OrderedDict()
dsx_attrs['_FillValue'] = -9999.0
dsx.swepercentile.attrs = dsx_attrs
dsx.attrs['analysis_date'] = date_unformat.strftime('%Y/%m/%d')
for attr in ['VIC_Model_Version', 'VIC_GIT_VERSION', 'VIC_Driver']:
    dsx.attrs[attr] = ds.attrs[attr]

# save to netcdf
dsx.to_netcdf(os.path.join(outfile_loc, 'vic-metdata_swepercentile_%s.nc' %
                           (date)), mode='w', format='NETCDF4')
