#!/usr/bin/env python
"""
get_seasonal_metfcst.py
usage: <python> <get_seasonal_metfcst.py> <configuration.cfg>
This script downloads downscaled NMME meteorological forecast data through
xarray from
http://proxy.nkn.uidaho.edu/thredds/catalog/
    NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/bcsd-nmme/dailyForecasts/
    catalog.html
delivered through OPeNDAP. Because attributes are lost during download,
they are added back in. To start, we just download multi-model ensemble mean.
"""
import xarray as xr
import os
import argparse
from cdo import Cdo
import cf_units

from tonic.io import read_config

# read in configuration file
parser = argparse.ArgumentParser(description='Download met forecast data')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

# initialize cdo
cdo = Cdo()

# read in meteorological data location
met_fcst_loc = config_dict['FORECAST']['SeasFcst_Met_Loc']
old_config_file = config_dict['ECFLOW']['old_Config']
new_config_file = config_dict['ECFLOW']['new_Config']

# read in grid_file from config file
grid_file = config_dict['SUBDAILY']['GridFile']

# define variable names used when filling threads URL
# an abbreviation and a full name is needed
varnames = ['was', 'dps', 'tasmean', 'tasmax', 'tasmin', 'rsds',
            'pr', 'pet']
# define model names for file name
modelnames = ['ENSMEAN']
# ['NCAR', 'NASA', 'GFDL', 'GFDL-FLOR', 'ENSMEAN', 'CMC1', 'CMC2',
#              'CFSv2']
new_units = {'was': 'm s-1', 'dps': 'degC', 'tasmean': 'degC',
             'tasmax': 'degC', 'tasmin': 'degC', 'rsds': 'W m-2',
             'pr': 'mm', 'pet': 'mm'}

# download metdata from http://thredds.northwestknowledge.net
for model in modelnames:
    dlist = []
    for var in varnames:
        url = ('http://tds-proxy.nkn.uidaho.edu/thredds/dodsC/' +
               'NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/bcsd-nmme/' +
               'dailyForecasts/' +
               'bcsd_nmme_metdata_%s_forecast_%s_daily.nc' % (
                   model, var))
        print('Reading {0}'.format(url))
        ds = xr.open_dataset(url)
        if ds[var].attrs['units'] == 'F':
            ds[var].attrs['units'] = 'degF'
        units_in = cf_units.Unit(ds[var].attrs['units'])
        units_out = cf_units.Unit(new_units[var])
        # Perform units conversion
        units_in.convert(ds[var].values[:], units_out, inplace=True)
        ds[var].attrs['units'] = new_units[var]
        dlist.append(ds)
    merge_ds = xr.merge(dlist)
    merge_ds = merge_ds.transpose('time', 'lat', 'lon')
    #tmp_file = os.path.join(met_fcst_loc, 'tmp_file.nc')
    #merge_ds.to_netcdf(tmp_file)
    outfile = os.path.join(met_fcst_loc, '%s.nc' % (model))
    print('Conservatively remap and write to {0}'.format(outfile))
    cdo.remapcon(grid_file, input=merge_ds, output=outfile)
