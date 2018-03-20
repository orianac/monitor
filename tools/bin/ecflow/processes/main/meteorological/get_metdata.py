#!/usr/bin/env python
'''
get_metdata.py
usage: <python> <get_metdata.py> <configuration.cfg>
This script downloads meteorological data through xarray from
http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_MET_catalog.html
delivered through OPeNDAP.
'''
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
    return ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/' +
            '{0}/{0}_{1}.nc?lon[0:1:{2}],lat[0:1:{3}],day[{4}:1:{5}],'.format(
                var[0], year, num_lon, num_lat, num_day[0], num_day[1]) +
            '{0}[{1}:1:{2}][0:1:{3}][0:1:{4}]'.format(
                var[1], num_day[0], num_day[1], num_lon, num_lat))


def recreate_attrs(met_ds):
    ''' add attributes back into data sets because xarray will receive error
        "Illegal attribute" when opening url and delete all attributes'''
    # set up some geographic information
    esri_str = ("GEOGCS[\\\"GCS_WGS_1984\\\",DATUM" +
                "[\\\"D_WGS_1984\\\",SPHEROID\\\"WGS_1984\\\"," +
                "6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0]," +
                "UNIT[\\\"Degree\\\",0.0174532925199433]]")
    for var in met_ds.variables:
        met_ds[var].attrs['_FillValue'] = -32767.
        met_ds[var].attrs['esri_pe_string'] = esri_str
        met_ds[var].attrs['coordinates'] = 'lon lat'
        met_ds[var].attrs['missing_value'] = -32767.
    # global attributes
    met_ds.attrs['author'] = ('John Abatzoglou - University of ' +
                              'Idaho, jabatzoglou@uidaho.edu')
    met_ds.attrs['date'] = datetime.now().strftime('%d %B %Y')
    met_ds.attrs['note1'] = ('The projection information for this ' +
                             'file is: GCS WGS 1984.')
    met_ds.attrs['note2'] = ('Citation: Abatzoglou, J.T., 2013, ' +
                             'Development of gridded surface ' +
                             'meteorological data for ecological ' +
                             'applications and modeling, International ' +
                             'Journal of Climatology, ' +
                             'DOI:10.1002/joc.3413')
    met_ds.attrs['last_permanent_slice'] = "50"
    met_ds.attrs['note3'] = ('Data in slices after ' +
                             'last_permanent_slice (1-based) are ' +
                             'considered provisional and subject to ' +
                             'change with subsequent updates')
    # latitude attributes
    met_ds.lat.attrs['units'] = "degrees_north"
    met_ds.lat.attrs['description'] = "latitude"
    # longitude attributes
    met_ds.lon.attrs['units'] = "degrees_east"
    met_ds.lon.attrs['description'] = "longitude"

    # time attributes
    met_ds.time.attrs['units'] = "days since 1900-01-01 00:00:00"
    met_ds.time.attrs['calendar'] = "gregorian"
    met_ds.time.attrs['description'] = "days since 1900-01-01"

    # parameter attributes
    # precipitation
    met_ds.precipitation_amount.attrs['units'] = "mm"
    met_ds.precipitation_amount.attrs['description'] = ('Daily Accumulation' +
                                                        ' Precipitation')
    met_ds.precipitation_amount.attrs['cell_methods'] = ('time: sum(' +
                                                         'intervals: 24 hours)')

    # temperature
    met_ds.tmmn.attrs['description'] = "Daily Minimum Temperature"
    met_ds.tmmx.attrs['description'] = "Daily Maximum Temperature"
    for var in ['tmmn', 'tmmx']:
        met_ds[var].attrs['units'] = "degC"
        met_ds[var].attrs['cell_methods'] = "time: sum(interval: 24 hours)"
        met_ds[var].attrs['height'] = "2 m"

    # wind speed
    met_ds.wind_speed.attrs['units'] = "m/s"
    met_ds.wind_speed.attrs['description'] = "Daily Mean Wind Speed"
    met_ds.wind_speed.attrs['height'] = "10 m"

    # shortwave radiation
    met_ds.surface_downwelling_shortwave_flux_in_air.attrs['units'] = "W m-2"
    met_ds.surface_downwelling_shortwave_flux_in_air.attrs['description'] = (
        'Daily Mean Downward Shortwave Radiation At Surface')

    # specific humidity
    met_ds.specific_humidity.attrs['units'] = "kg/kg"
    met_ds.specific_humidity.attrs['description'] = "Daily Mean Specific Humidity"
    met_ds.specific_humidity.attrs['height'] = "2 m"

    return met_ds


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

    # get units conversion from cf_units for K to degC
    units_in = cf_units.Unit('K')
    units_out = cf_units.Unit('degC')

    # initial cdo
    cdo = Cdo()

    # read in meteorological data location
    old_config_file = config_dict['ECFLOW']['old_Config']
    new_config_file = config_dict['ECFLOW']['new_Config']
    n_days = int(config_dict['ECFLOW']['Met_Delay'])
    met_out = config_dict['ECFLOW']['Orig_Met']

    # read in grid_file from config file
    grid_file = config_dict['SUBDAILY']['GridFile']

    # get current date and number of days
    # define the end date for subdaily generation and the vic run
    end_date = datetime.now() - timedelta(days=n_days)
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
    varnames = [('pr', 'precipitation_amount'), ('tmmn', 'air_temperature'),
                ('tmmx', 'air_temperature'), ('vs', 'wind_speed'),
                ('srad', 'surface_downwelling_shortwave_flux_in_air'),
                ('sph', 'specific_humidity')]

    # check if data we are downloading coms from multiple years
    # replace start date, end date and met location in the configuration file
    kwargs = {'SUBD_MET_START_DATE': vic_start_date_format,
              'END_DATE': end_date_format, 'VIC_START_DATE': vic_start_date_format,
              'VIC_SAVE_STATE': vic_save_state_format,
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
            # download data and add general attributes
            xds = xr.open_dataset(url)

            # place data in dict, the variable abbreviation is used as key
            met_dsets[var[0]] = xds

    for var in ('tmmn', 'tmmx'):
        # Perform units conversion
        units_in.convert(met_dsets[var].air_temperature.values[:], units_out,
                         inplace=True)
        # Fix _FillValue after unit conversion
        met_dsets[var].air_temperature.values[
            met_dsets[var].air_temperature < -30000] = -32767.
        # Change variable names so that tmmn and tmax are different
        met_dsets[var].rename({'air_temperature': var}, inplace=True)
    merge_ds = xr.merge(list(met_dsets.values()))
    merge_ds.transpose('day', 'lat', 'lon')
    # MetSim requires time dimension be named "time"
    merge_ds.rename({'day': 'time'}, inplace=True)
    # add attributes
    merge_ds = recreate_attrs(merge_ds)
    # Make sure tmax >= tmin always
    tmin = np.copy(merge_ds['tmmn'].values)
    tmax = np.copy(merge_ds['tmmx'].values)
    swap_values = ((tmin > tmax) & (tmax != -32767.))
    merge_ds['tmmn'].values[swap_values] = tmax[swap_values]
    merge_ds['tmmx'].values[swap_values] = tmin[swap_values]

    # conservatively remap to grid file
    cdo.remapcon(grid_file, input=merge_ds, output=met_out)


if __name__ == "__main__":
    main()
