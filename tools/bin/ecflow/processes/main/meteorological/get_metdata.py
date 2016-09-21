#!/usr/bin/env python
"""
get_metdata.py 
usage: <python> <get_metdata.py> <configuration.cfg>
This script downloads meteorological data through xarray from
http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_MET_catalog.html
delivered through OPeNDAP. Because attributes are lost during download, 
they are added back in. 
"""
import xarray as xr
import os
import argparse
import calendar
from collections import OrderedDict
from datetime import datetime, timedelta
from tonic.io import read_config
from monitor import model_tools
######### ----------------------------------------###########

# read in configuration file
parser = argparse.ArgumentParser(description='Download met data')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

# read in meterological data location
full_year_met_loc = config_dict['SUBDAILY']['Full_Year_Met_Data']
daily_met_loc = config_dict['SUBDAILY']['Daily_Met_Data']
old_config_file = config_dict['ECFLOW']['old_Config']
new_config_file = config_dict['ECFLOW']['new_Config']
n_days = int(config_dict['ECFLOW']['Met_Delay'])

# get date and number of days
date = datetime.now() - timedelta(days=n_days)
num_day = date.timetuple().tm_yday - 1  # python 0 start correction
date_format = date.strftime('%Y-%m-%d')
year = date.strftime('%Y')

lastyear_date = datetime.now() - timedelta(days=365 + n_days)
lastyear_num_day = lastyear_date.timetuple().tm_yday - 1
lastyear_date_format = lastyear_date.strftime('%Y-%m-%d')
lastyear = lastyear_date.strftime('%Y')

# define the end date used in the subdaily generation
subd_met_end_date = date.strftime('%Y-%m-%d')

num_startofyear = 0
if calendar.isleap(int(lastyear)):
    num_endofyear = 365
else:
    num_endofyear = 364

# set a variable name for the number of lats and lons
num_lat = 584
num_lon = 1385

# define variable names used when filling threads URL
varnames = [('pr', 'precipitation_amount'), ('tmmn', 'air_temperature'),
            ('tmmx', 'air_temperature'), ('vs', 'wind_speed'),
            ('srad', 'surface_downwelling_shortwave_flux_in_air'), ('sph', 'specific_humidity')]

# create attribute dictionaries

# global attributes
datestring = datetime.now()
today_date = datestring.strftime('%d %B %Y')

globe_attrs = OrderedDict()
globe_attrs[
    'author'] = "John Abatzoglou - University of Idaho, jabatzoglou@uidaho.edu"
globe_attrs['date'] = today_date
globe_attrs[
    'note1'] = "The projection information for this file is: GCS WGS 1984."
globe_attrs['note2'] = ("Citation: Abatzoglou, J.T., 2013, Development of gridded surface " +
                        "meteorological data for ecological applications and modeling, " +
                        "International Journal of Climatology, DOI: 10.1002/joc.3413")
globe_attrs['last_permanent_slice'] = "50"
globe_attrs['note3'] = ("Data in slices after last_permanent_slice (1-based) are " +
                        "considered provisional and subject to change with subsequent updates")
# latitude attributes
lat_attrs = OrderedDict()
lat_attrs['units'] = "degrees_north"
lat_attrs['description'] = "latitude"

# longitude attributes
lon_attrs = OrderedDict()
lon_attrs['units'] = "degrees_east"
lon_attrs['description'] = "longitude"

# time attributes
day_attrs = OrderedDict()
day_attrs['units'] = "days since 1900-01-01 00:00:00"
day_attrs['calendar'] = "gregorian"
day_attrs['description'] = "days since 1900-01-01"

esri_str = ("GEOGCS[\\\"GCS_WGS_1984\\\",DATUM" +
            "[\\\"D_WGS_1984\\\",SPHEROID\\\"WGS_1984\\\"," +
            "6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0]," +
            "UNIT[\\\"Degree\\\",0.0174532925199433]]")

# parameter attributes
# precipitation
pr_attrs = OrderedDict()
pr_attrs['units'] = "mm"
pr_attrs['description'] = "Daily Accumulation Precipitation"
pr_attrs['_FillValue'] = -32767.
pr_attrs['esri_pe_string'] = esri_str
pr_attrs['coordinates'] = "lon lat"
pr_attrs['cell_methods'] = "time: sum(intervals: 24 hours)"
pr_attrs['missing_value'] = -32767.

# minimum temperature
tmmn_attrs = OrderedDict()
tmmn_attrs['units'] = "K"
tmmn_attrs['description'] = "Daily Minimum Temperature"
tmmn_attrs['_FillValue'] = -32767.
tmmn_attrs['esri_pe_string'] = esri_str
tmmn_attrs['coordinates'] = "lon lat"
tmmn_attrs['cell_methods'] = "time: sum(interval: 24 hours)"
tmmn_attrs['height'] = "2 m"
tmmn_attrs['missing_value'] = -32767.

# maximum temperature
tmmx_attrs = OrderedDict()
tmmx_attrs['units'] = "K"
tmmx_attrs['description'] = "Daily Maximum Temperature"
tmmx_attrs['_FillValue'] = -32767.
tmmx_attrs['esri_pe_string'] = esri_str
tmmx_attrs['coordinates'] = "lon lat"
tmmx_attrs['cell_methods'] = "time: sum(interval: 24 hours)"
tmmx_attrs['height'] = "2 m"
tmmx_attrs['missing_value'] = -32767.

# wind speed
vs_attrs = OrderedDict()
vs_attrs['units'] = "m/s"
vs_attrs['description'] = "Daily Mean Wind Speed"
vs_attrs['_FillValue'] = -32767.
vs_attrs['esri_pe_string'] = esri_str
vs_attrs['coordinates'] = "lon lat"
vs_attrs['height'] = "10 m"
vs_attrs['missing_value'] = -32767.

# shortwave radiation
srad_attrs = OrderedDict()
srad_attrs['units'] = "W m-2"
srad_attrs['description'] = "Daily Mean Downward Shortwave Radiation At Surface"
srad_attrs['_FillValue'] = -32767.
srad_attrs['esri_pe_string'] = esri_str
srad_attrs['coordinates'] = "lon lat"
srad_attrs['missing_value'] = -32767.

# specific humidity
sph_attrs = OrderedDict()
sph_attrs['units'] = "kg/kg"
sph_attrs['description'] = "Daily Mean Specific Humidity"
sph_attrs['_FillValue'] = -32767.
sph_attrs['esri_pe_string'] = esri_str
sph_attrs['coordinates'] = "lon lat"
sph_attrs['height'] = "2 m"
sph_attrs['missing_value'] = -32767.

# if the full year met folder is empty, we must download the full year
if not os.listdir(full_year_met_loc):

    # replace start date, end date and met location in the configuration file
    kwargs = {'MODEL_DATE': date_format, 'SUBD_MET_START_DATE': lastyear_date_format,
              'SUBD_MET_END_DATE': subd_met_end_date, 'MET_LOC': full_year_met_loc,
              'FULL_YEAR': 'Year'}
    model_tools.replace_var_pythonic_config(
        old_config_file, new_config_file, header=None, **kwargs)

    met_loc = full_year_met_loc

    # download metdata from http://thredds.northwestknowledge.net
    met_dsets = dict()
    for var in varnames:
        url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                        "/thredds/dodsC/MET/%s/%s_%s.nc?lon[0:1:%s]," % (var[0], var[0], year, num_lon) +
                        "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_startofyear, num_day) +
                        "%s[%s:1:%s]" % (var[1], num_startofyear, num_day) +
                        "[0:1:%s][0:1:%s]" % (num_lon, num_lat))

        ds_thisyear = xr.open_dataset(url_thisyear)

        url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                        "/thredds/dodsC/MET/%s/%s_%s.nc?lon[0:1:%s]," % (var[0], var[0], lastyear, num_lon) +
                        "lat[0:1:%s],day[%s:1:%s]," % (num_lat, lastyear_num_day, num_endofyear) +
                        "%s[%s:1:%s]" % (var[1], lastyear_num_day, num_endofyear) +
                        "[0:1:%s][0:1:%s]" % (num_lon, num_lat))

        ds_lastyear = xr.open_dataset(url_lastyear)

        # concatenate the two datasets and add general attributes
        ds = xr.concat([ds_lastyear, ds_thisyear], 'day')
        ds.lat.attrs = lat_attrs
        ds.lon.attrs = lon_attrs
        ds.day.attrs = day_attrs
        ds.attrs = globe_attrs

        # place data in dict
        met_dsets[var[0]] = ds


else:  # if met data for the past year has been downloaded, we only have to download one day at a time.

    # replace start date, end date and met location in the configuration file
    kwargs = {'MODEL_DATE': date_format, 'SUBD_MET_START_DATE': lastyear_date_format,
              'SUBD_MET_END_DATE': subd_met_end_date, 'MET_LOC': daily_met_loc,
              'FULL_YEAR': 'Day'}
    model_tools.replace_var_pythonic_config(
        old_config_file, new_config_file, header=None, **kwargs)

    met_loc = daily_met_loc

    # download metdata from http://thredds.northwestknowledge.net
    met_dsets = dict()
    for var in varnames:
        url = ("http://thredds.northwestknowledge.net:8080" +
               "/thredds/dodsC/MET/%s/%s_%s.nc?lon[0:1:%s]," % (var[0], var[0], year, num_lon) +
               "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_day, num_day) +
               "%s[%s:1:%s]" % (var[1], num_day, num_day) +
               "[0:1:%s][0:1:%s]" % (num_lon, num_lat))

        # download data and add general attributes
        ds = xr.open_dataset(url)
        ds.lat.attrs = lat_attrs
        ds.lon.attrs = lon_attrs
        ds.day.attrs = day_attrs
        ds.attrs = globe_attrs

        # place data in dict
        met_dsets[var[0]] = ds

# add variable specific attributes and save as netcdf
met_dsets['pr'].precipitation_amount.attrs = pr_attrs
met_dsets['pr'].to_netcdf(os.path.join(met_loc, 'pr.nc'),
                          mode='w', format='NETCDF4')

met_dsets['tmmn'].air_temperature.attrs = tmmn_attrs
met_dsets['tmmn'].to_netcdf(os.path.join(met_loc, 'tmmn.nc'),
                            mode='w', format='NETCDF4')

met_dsets['tmmx'].air_temperature.attrs = tmmx_attrs
met_dsets['tmmx'].to_netcdf(os.path.join(met_loc, 'tmmx.nc'),
                            mode='w', format='NETCDF4')

met_dsets['vs'].wind_speed.attrs = vs_attrs
met_dsets['vs'].to_netcdf(os.path.join(met_loc, 'vs.nc'),
                          mode='w', format='NETCDF4')

met_dsets['srad'].surface_downwelling_shortwave_flux_in_air.attrs = srad_attrs
met_dsets['srad'].to_netcdf(os.path.join(met_loc, 'srad.nc'),
                            mode='w', format='NETCDF4')

met_dsets['sph'].specific_humidity.attrs = sph_attrs
met_dsets['sph'].to_netcdf(os.path.join(met_loc, 'sph.nc'),
                           mode='w', format='NETCDF4')
