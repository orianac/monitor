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
from collections import OrderedDict
from datetime import datetime, timedelta
from tonic.io import read_config
from monitor import model_tools
######### ----------------------------------------###########

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FileType('r'),
                    nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in meterological data location
met_loc = config_dict['ECFLOW']['Met_Loc']
old_config_file = config_dict['ECFLOW']['old_Config']
new_config_file = config_dict['ECFLOW']['new_Config']
N = int(config_dict['ECFLOW']['Met_Delay'])

# get date and number of days
date = datetime.now() - timedelta(days=N)
num_day = date.timetuple().tm_yday - 1  # python 0 start correction
num_startofyear = 0
num_endofyear = 364
year = date.strftime('%Y')
lastyear = date.timetuple().tm_year - 1
date_format = date.strftime('%Y-%m-%d')
lastyear_date = datetime.now() - timedelta(days=365)
lastyear_date_format = lastyear_date.strftime('%Y-%m-%d')

# replace start date and end date in the configuration file
kwargs = {'MODEL_START_DATE': date_format, 'MODEL_END_DATE': date_format, 'SUBD_MET_START_DATE': lastyear_date_format}
model_tools.replace_var_pythonic_config(
    old_config_file, new_config_file, header=None, **kwargs)

# set a variable name for the number of lats and lons
num_lat = 584
num_lon = 1385

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

# download metdata from http://thredds.northwestknowledge.net
# precipitation
pr_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                   "/thredds/dodsC/MET/pr/pr_%s.nc?lon[0:1:%s]," % (year, num_lon) +
                   "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_startofyear, num_day) +
                   "precipitation_amount[%s:1:%s]" % (num_startofyear, num_day) +
                   "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
pr_thisyear_ds = xr.open_dataset(pr_url_thisyear)

pr_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                   "/thredds/dodsC/MET/pr/pr_%s.nc?lon[0:1:%s]," % (lastyear, num_lon) +
                   "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_day, num_endofyear) +
                   "precipitation_amount[%s:1:%s]" % (num_day, num_endofyear) +
                   "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
pr_lastyear_ds = xr.open_dataset(pr_url_lastyear)

# concatenate the two files
pr_total_ds = xr.concat([pr_lastyear_ds, pr_thisyear_ds], 'day')

# add attributes (these are include the same descriptions as can be found from URL
# this information does not get downloaded but is necessary for CDO
# commands and tonic
pr_total_ds.precipitation_amount.attrs = pr_attrs
pr_total_ds.lat.attrs = lat_attrs
pr_total_ds.lon.attrs = lon_attrs
pr_total_ds.day.attrs = day_attrs
pr_total_ds.attrs = globe_attrs
# save netcdf
pr_total_ds.to_netcdf(os.path.join(met_loc, 'pr_total.nc'),
                mode='w', format='NETCDF4')

# minimum temperature
tmmn_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/tmmn/tmmn_%s.nc?lon[0:1:%s]," % (year, num_lon) +
                     "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_startofyear, num_day) +
                     "air_temperature[%s:1:%s]" % (num_startofyear, num_day) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
tmmn_thisyear_ds = xr.open_dataset(tmmn_url_thisyear)

tmmn_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/tmmn/tmmn_%s.nc?lon[0:1:%s]," % (lastyear, num_lon) +
                     "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_day, num_endofyear) +
                     "air_temperature[%s:1:%s]" % (num_day, num_endofyear) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
tmmn_lastyear_ds = xr.open_dataset(tmmn_url_lastyear)

tmmn_total_ds = xr.concat([tmmn_lastyear_ds, tmmn_thisyear_ds], 'day')

tmmn_total_ds.air_temperature.attrs = tmmn_attrs
tmmn_total_ds.lat.attrs = lat_attrs
tmmn_total_ds.lon.attrs = lon_attrs
tmmn_total_ds.day.attrs = day_attrs
tmmn_total_ds.attrs = globe_attrs

tmmn_total_ds.to_netcdf(os.path.join(met_loc, 'tmmn_total.nc'),
                           mode='w', format='NETCDF4')

# maximum temperature
tmmx_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/tmmx/tmmx_%s.nc?lon[0:1:%s]," % (year, num_lon) +
                     "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_startofyear, num_day) +
                     "air_temperature[%s:1:%s]" % (num_startofyear, num_day) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
tmmx_thisyear_ds = xr.open_dataset(tmmx_url_thisyear)

tmmx_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/tmmx/tmmx_%s.nc?lon[0:1:%s]," % (lastyear, num_lon) +
                     "lat[0:1:%s],day[%s:1:%s]," % (num_lat, num_day, num_endofyear) +
                     "air_temperature[%s:1:%s]" % (num_day, num_endofyear) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
tmmx_lastyear_ds = xr.open_dataset(tmmx_url_lastyear)

tmmx_total_ds = xr.concat([tmmx_lastyear_ds, tmmx_thisyear_ds], 'day')

tmmx_total_ds.air_temperature.attrs = tmmx_attrs
tmmx_total_ds.lat.attrs = lat_attrs
tmmx_total_ds.lon.attrs = lon_attrs
tmmx_total_ds.day.attrs = day_attrs
tmmx_total_ds.attrs = globe_attrs
tmmx_total_ds.to_netcdf(os.path.join(met_loc, 'tmmx_total.nc'),
                           mode='w', format='NETCDF4')

# wind speed
vs_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                   "/thredds/dodsC/MET/vs/vs_%s.nc?lon[0:1:%s]," % (year, num_lon) +
                   "lat[0:1:584],day[%s:1:%s]," % (num_startofyear, num_day) +
                   "wind_speed[%s:1:%s]" % (num_startofyear, num_day) +
                   "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
vs_thisyear_ds = xr.open_dataset(vs_url_thisyear)

vs_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                   "/thredds/dodsC/MET/vs/vs_%s.nc?lon[0:1:%s]," % (lastyear, num_lon) +
                   "lat[0:1:584],day[%s:1:%s]," % (num_day, num_endofyear) +
                   "wind_speed[%s:1:%s]" % (num_day, num_endofyear) +
                   "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
vs_lastyear_ds = xr.open_dataset(vs_url_lastyear)

vs_total_ds = xr.concat([vs_lastyear_ds, vs_thisyear_ds], 'day')

vs_total_ds.wind_speed.attrs = vs_attrs
vs_total_ds.lat.attrs = lat_attrs
vs_total_ds.lon.attrs = lon_attrs
vs_total_ds.day.attrs = day_attrs
vs_total_ds.attrs = globe_attrs
vs_total_ds.to_netcdf(os.path.join(met_loc, 'vs_total.nc'), 
		mode='w', format='NETCDF4')

# shortwave radiation
srad_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/srad/srad_%s.nc?lon[0:1:%s]," %(year, num_lon) +
                     "lat[0:1:584],day[%s:1:%s]," %(num_startofyear, num_day) +
                     "surface_downwelling_shortwave_flux_in_air[%s:1:%s]" %(num_startofyear, num_day) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
srad_thisyear_ds = xr.open_dataset(srad_url_thisyear)

srad_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                     "/thredds/dodsC/MET/srad/srad_%s.nc?lon[0:1:%s]," %(year, num_lon) +
                     "lat[0:1:584],day[%s:1:%s]," %(num_startofyear, num_day) +
                     "surface_downwelling_shortwave_flux_in_air[%s:1:%s]" %(num_startofyear, num_day) +
                     "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
srad_lastyear_ds = xr.open_dataset(srad_url_lastyear)

srad_total_ds = xr.concat([srad_lastyear_ds, srad_thisyear_ds], 'day')

srad_total_ds.surface_downwelling_shortwave_flux_in_air.attrs = srad_attrs
srad_total_ds.lat.attrs = lat_attrs
srad_total_ds.lon.attrs = lon_attrs
srad_total_ds.day.attrs = day_attrs
srad_total_ds.attrs = globe_attrs
srad_total_ds.to_netcdf(os.path.join(met_loc, 'srad_total.nc'),
        	           mode='w', format='NETCDF4')

#specific humidity
sph_url_thisyear = ("http://thredds.northwestknowledge.net:8080" +
                    "/thredds/dodsC/MET/sph/sph_%s.nc?lon[0:1:%s]," %(year, num_lon) +
                    "lat[0:1:584],day[%s:1:%s]," %(num_startofyear, num_day) +
                    "specific_humidity[%s:1:%s]" %(num_startofyear, num_day) +
                    "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
sph_thisyear_ds = xr.open_dataset(sph_url_thisyear)

sph_url_lastyear = ("http://thredds.northwestknowledge.net:8080" +
                    "/thredds/dodsC/MET/sph/sph_%s.nc?lon[0:1:%s]," %(lastyear, num_lon) +
                    "lat[0:1:584],day[%s:1:%s]," %(num_day, num_endofyear) +
                    "specific_humidity[%s:1:%s]" %(num_day, num_endofyear) +
                    "[0:1:%s][0:1:%s]" % (num_lon, num_lat))
sph_lastyear_ds = xr.open_dataset(sph_url_lastyear)

sph_total_ds = xr.concat([sph_lastyear_ds, sph_thisyear_ds], 'day')

sph_total_ds.specific_humidity.attrs = sph_attrs
sph_total_ds.lat.attrs = lat_attrs
sph_total_ds.lon.attrs = lon_attrs
sph_total_ds.day.attrs = day_attrs
sph_total_ds.attrs = globe_attrs
sph_total_ds.to_netcdf(os.path.join(met_loc, 'sph_total.nc'),
		          mode='w', format='NETCDF4')

