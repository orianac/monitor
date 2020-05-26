#!/usr/bin/env python
'''
get_metdata.py
usage: <python> <get_metdata.py> <configuration.cfg>
This script downloads meteorological data through xarray from
http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_MET_catalog.html
delivered through OPeNDAP.
'''
import os
import argparse
import calendar
from datetime import datetime, timedelta
import cf_units
from cdo import Cdo
import xarray as xr
import numpy as np

from tonic.io import read_config
from monitor import model_tools


def define_url(var, year, num_lon, num_lat, num_day):
    ''' create url for Northwest Knowlegdge Network gridMet download '''
    #return ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/tmmx/tmmx_2019.nc?lon[30:30:1],lat[1:1:40],day[1:1:1],crs[0:1:0],air_temperature[30:30:1][1:1:40][1:1:1]')
    return ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/' +
    #return ('http://reacchdb.nkn.uidaho.edu:/space/obj1/netcdf/MET/data/' +
            '{0}/{0}_{1}.nc?lon[0:1:{2}],lat[0:1:{3}],day[{4}:1:{5}],'.format(
                var[0], year, num_lon, num_lat, num_day[0], num_day[1]) +
            'crs[0:1:0],{0}[{1}:1:{2}][0:1:{3}][0:1:{4}]'.format(
            #'{0}[{1}:1:{2}][0:1:{3}][0:1:{4}]'.format(
                var[1], num_day[0], num_day[1], num_lat, num_lon))


def main():
    '''
    Download data from
    http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_MET_catalog.html
    delivered through OPeNDAP. Because attributes are lost during download,
    they are added back in.
    '''
    # read in configuration file
    parser = argparse.ArgumentParser(description='Download met data')
    parser.add_argument('config_file', metavar='config_file',
                        help='configuration file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)

    # initialize cdo
    cdo = Cdo()

    # read in meteorological data location
    old_config_file = config_dict['ECFLOW']['old_Config']
    new_config_file = config_dict['ECFLOW']['new_Config']
    n_days = int(config_dict['ECFLOW']['Met_Delay'])
    met_out = config_dict['MONITOR']['Orig_Met']

    # read in grid_file from config file
    grid_file = config_dict['DOMAIN']['GridFile']

    # get current date and number of days
    # define the end date for subdaily generation and the vic run
    end_date = datetime.now() - timedelta(days=n_days)
    next_day = end_date + timedelta(days=1)
    end_date_format = end_date.strftime('%Y-%m-%d')
    num_day = end_date.timetuple().tm_yday - 1  # python 0 start correction

    # get VIC start date and the date to save the state
    # the last 60 days in the met dataset are provisional, so we start our run
    # the day before the first provisional day
    vic_start_date = end_date - timedelta(days=60)
    vic_start_date_format = vic_start_date.strftime('%Y-%m-%d')
    # python 0 start correction
    vic_start_date_num = vic_start_date.timetuple().tm_yday - 1
    # we save the state at the beginning of the first provisional day
    vic_save_state = vic_start_date + timedelta(days=1)
    vic_save_state_format = vic_save_state.strftime('%Y-%m-%d')

    num_startofyear = 0
    if calendar.isleap(vic_start_date.year):
        num_endofyear = 365
    else:
        num_endofyear = 364

    # set a variable name for the number of lats and lons
    num_lat = 584
    num_lon = 1385

    # define variable names used when filling threads URL
    # an abbreviation and a full name is needed
    varnames = [('pr', 'precipitation_amount'),
                ('tmmn', 'air_temperature'),
                ('tmmx', 'air_temperature'), ('vs', 'wind_speed'),
                ('srad', 'surface_downwelling_shortwave_flux_in_air'),
                ('sph', 'specific_humidity')]

    # check if data we are downloading comes from multiple years
    # replace start date, end date and met location in the configuration file
    kwargs = {'START_DATE': vic_start_date_format,
              'END_DATE': end_date_format,
              'VIC_SAVE_STATE': vic_save_state_format,
              'MED_START_DATE': next_day.strftime('%Y-%m-%d'),
              'MED_END_DATE': '-9999',
              'MED_VIC_SAVE_STATE': '-9999',
              'SEAS_START_DATE': '-9999',
              'SEAS_END_DATE': '-9999',
              'SEAS_VIC_SAVE_STATE': '-9999',
              'FULL_YEAR': 'Year'}
    model_tools.replace_var_pythonic_config(old_config_file, new_config_file,
                                            header=None, **kwargs)
    met_dsets = dict()
    if vic_start_date.year != end_date.year:
        num_startofyear = 0
        if calendar.isleap(vic_start_date.year):
            num_endofyear = 365
        else:
            num_endofyear = 364
        # download metdata from http://thredds.northwestknowledge.net
        for var in varnames:
            url_thisyear = define_url(var, end_date.year, num_lon, num_lat,
                                      [num_startofyear, num_day])

            ds_thisyear = xr.open_dataset(url_thisyear)
            print(url_thisyear)
            url_lastyear = define_url(var, vic_start_date.year, num_lon, num_lat,
                                      [vic_start_date_num, num_endofyear])
            ds_lastyear = xr.open_dataset(url_lastyear)

            # concatenate the two datasets and add general attributes
            xds = xr.concat([ds_lastyear, ds_thisyear], 'day')

            # place data in dict, the variable abbreviation is used as key
            met_dsets[var[0]] = xds
    else:
        # download metdata from http://thredds.northwestknowledge.net
        for var in varnames:

            url = define_url(var, end_date.year, num_lon, num_lat,
                             [vic_start_date_num, num_day])
            print(url)
            # download data and add general attributes
            xds = xr.open_dataset(url)

            # place data in dict, the variable abbreviation is used as key
            met_dsets[var[0]] = xds

    for var in ('tmmn', 'tmmx'):
        # Perform units conversion
        print('in the unit conversion')
        units_in = cf_units.Unit(met_dsets[var].air_temperature.attrs['units'])
        units_out = cf_units.Unit('degC')
        print(met_dsets[var])
        units_in.convert(met_dsets[var].air_temperature.values[:], units_out,
                         inplace=True)
        # Fix _FillValue after unit conversion
        print('here!')
        print(met_dsets[var].air_temperature < -30000)
        print(met_dsets[var].air_temperature.values.shape)
        met_dsets[var].air_temperature.values[
            met_dsets[var].air_temperature < -30000] = -32767.
        # Change variable names so that tmmn and tmax are different
        met_dsets[var].rename({'air_temperature': var}, inplace=True)
        met_dsets[var].attrs['units'] = "degC"
    print('before the merge')
    merge_ds = xr.merge(list(met_dsets.values()))
    print('right after merge')
    merge_ds = merge_ds.drop('crs')
    print('after the merge')
    merge_ds.transpose('day', 'lat', 'lon')
    print('after transpose')
    # MetSim requires time dimension be named "time"
    merge_ds.rename({'day': 'time'}, inplace=True)
    # Make sure tmax >= tmin always
    tmin = np.copy(merge_ds['tmmn'].values)
    tmax = np.copy(merge_ds['tmmx'].values)
    print('before swap')
    swap_values = ((tmin > tmax) & (tmax != -32767.))
    nswap = np.sum(swap_values)
    if nswap > 0:
        print('MINOR WARNING: tmax < tmin in {} cases'.format(nswap))
    merge_ds['tmmn'].values[swap_values] = tmax[swap_values]
    merge_ds['tmmx'].values[swap_values] = tmin[swap_values]
    # write merge_ds to a temporary file so that we don't run into
    # issues with the system /tmp directoy filling up
    temporary = os.path.join(config_dict['ECFLOW']['TempDir'], 'met_out')
    merge_ds.to_netcdf(temporary)
    print('writing out to {0} mapped to {1}'.format(met_out, grid_file))
    # conservatively remap to grid file
    cdo.remapcon(grid_file, input=temporary, output=met_out)
    os.remove(temporary)
    ds = xr.open_dataset(met_out).load()
    ds.close()
    tmin = np.copy(ds['tmmn'].values)
    tmax = np.copy(ds['tmmx'].values)
    swap_values = ((tmin > tmax) & (tmax != -32767.))
    nswap = np.sum(swap_values)
    if nswap > 0:
        print('MINOR WARNING: tmax < tmin in {} cases'.format(nswap))
    ds['tmmn'].values[swap_values] = tmax[swap_values]
    ds['tmmx'].values[swap_values] = tmin[swap_values]
    ds.to_netcdf(met_out)

if __name__ == "__main__":
    main()
