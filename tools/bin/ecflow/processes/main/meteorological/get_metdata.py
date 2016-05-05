#!/usr/bin/env python
#####get_metdata.py 
#####usage: <python> <get_metdata.py> <configuration.cfg>

###This script downloads meteorological data through xarray from
###http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_MET_catalog.html
###delivered through OPeNDAP.

import sys
import numpy as np
import xarray as xr
import os
import time
import argparse
from collections import OrderedDict
from datetime import datetime, timedelta
from tonic.io import read_config
from monitor.io import replace, proc_subprocess

######### ----------------------------------------###########

#read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

#read in meterological data location
met_loc = config_dict['ECFLOW']['Met_Loc']
config_file = config_dict['ECFLOW']['Config'] 
N = config_dict['ECFLOW']['Met_Delay']

N = int(N)

#get date and number of days
date = datetime.now() - timedelta(days=N)
num_day = date.timetuple().tm_yday - 1 #python 0 start correction
year = date.strftime('%Y')
date_format = date.strftime('%Y-%m-%d')

#create attribute dictionaries 

#global attributes
datestring = datetime.now()   
today_date = datestring.strftime('%d %B %Y')

globe_attrs = {}
globe_attrs['author'] =   "John Abatzoglou - University of Idaho, jabatzoglou@uidaho.edu" 
globe_attrs['date'] = "%s" %(today_date) 
globe_attrs['note1'] = "The projection information for this file is: GCS WGS 1984." 
globe_attrs['note2'] = "Citation: Abatzoglou, J.T., 2013, Development of gridded surface meteorological data for ecological applications and modeling, International Journal of Climatology, DOI: 10.1002/joc.3413" 
globe_attrs['last_permanent_slice'] = "50"
globe_attrs['note3'] = "Data in slices after last_permanent_slice (1-based) are considered provisional and subject to change with subsequent updates" 

#latitude attributes
lat_attrs = {}
lat_attrs['units'] = "degrees_north"
lat_attrs['description'] = "latitude"

#longitude attributes
lon_attrs = {}
lon_attrs['units'] = "degrees_east"
lon_attrs['description'] = "longitude"

#time attributes
day_attrs = {}
day_attrs['units'] = "days since 1900-01-01 00:00:00"
day_attrs['calendar'] = "gregorian"
day_attrs['description'] = "days since 1900-01-01"

#parameter attributes
#precipitation
pr_attrs = {}
pr_attrs['units'] = "mm"
pr_attrs['description'] = "Daily Accumulation Precipitation"
pr_attrs['_FillValue'] = -32767.
pr_attrs['esri_pe_string'] = "GEOGCS[\\\"GCS_WGS_1984\\\",DATUM[\\\"D_WGS_1984\\\",SPHEROID[\\\"WGS_1984\\\",6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0],UNIT[\\\"Degree\\\",0.0174532925199433]]" 
pr_attrs['coordinates'] = "lon lat" 
pr_attrs['cell_methods'] = "time: sum(intervals: 24 hours)"
pr_attrs['missing_value'] = -32767.

#minimum temperature
tmmn_attrs = {}
tmmn_attrs['units'] = "K"
tmmn_attrs['description'] = "Daily Minimum Temperature"
tmmn_attrs['_FillValue'] = -32767.
tmmn_attrs['esri_pe_string'] = "GEOGCS[\\\"GCS_WGS_1984\\\",DATUM[\\\"D_WGS_1984\\\",SPHEROID[\\\"WGS_1984\\\",6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0],UNIT[\\\"Degree\\\",0.0174532925199433]]" 
tmmn_attrs['coordinates'] = "lon lat" 
tmmn_attrs['cell_methods'] = "time: sum(interval: 24 hours)"
tmmn_attrs['height'] = "2 m"
tmmn_attrs['missing_value'] = -32767.

#maximum temperature
tmmx_attrs = {}
tmmx_attrs['units'] = "K"
tmmx_attrs['description'] = "Daily Maximum Temperature"
tmmx_attrs['_FillValue'] = -32767.
tmmx_attrs['esri_pe_string'] = "GEOGCS[\\\"GCS_WGS_1984\\\",DATUM[\\\"D_WGS_1984\\\",SPHEROID[\\\"WGS_1984\\\",6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0],UNIT[\\\"Degree\\\",0.0174532925199433]]" 
tmmx_attrs['coordinates'] = "lon lat" 
tmmx_attrs['cell_methods'] = "time: sum(interval: 24 hours)"
tmmx_attrs['height'] = "2 m"
tmmx_attrs['missing_value'] = -32767.

#wind speed
vs_attrs = {}
vs_attrs['units'] = "m/s"
vs_attrs['description'] = "Daily Mean Wind Speed"
vs_attrs['_FillValue'] = -32767.
vs_attrs['esri_pe_string'] = "GEOGCS[\\\"GCS_WGS_1984\\\",DATUM[\\\"D_WGS_1984\\\",SPHEROID[\\\"WGS_1984\\\",6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0],UNIT[\\\"Degree\\\",0.0174532925199433]]" 
vs_attrs['coordinates'] = "lon lat" 
vs_attrs['height'] = "10 m"
vs_attrs['missing_value'] = -32767.


#download metdata from http://thredds.northwestknowledge.net
#precipitation
pr_ds = xr.open_dataset("http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/pr/pr_%s.nc?lon[0:1:1385],lat[0:1:584],day[%s:1:%s],precipitation_amount[%s:1:%s][0:1:1385][0:1:584]" %(year, num_day, num_day, num_day, num_day))
#add attributes (these are include the same descriptions as can be found from URL
#this information does not get downloaded but is necessary for CDO commands and tonic
pr_ds.precipitation_amount.attrs = OrderedDict(pr_attrs)
pr_ds.lat.attrs = OrderedDict(lat_attrs)
pr_ds.lon.attrs = OrderedDict(lon_attrs)
pr_ds.day.attrs = OrderedDict(day_attrs)
pr_ds.attrs = OrderedDict(globe_attrs)
#save netcdf
pr_ds.to_netcdf('/raid3/mbapt/ecflow/pnw/met_files/pr.nc', mode='w', format='NETCDF4')
print("precipitation netcdf saved")

#minimum temperature
tmmn_ds = xr.open_dataset("http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/tmmn/tmmn_%s.nc?lon[0:1:1385],lat[0:1:584],day[%s:1:%s],air_temperature[%s:1:%s][0:1:1385][0:1:584]" %(year, num_day, num_day, num_day, num_day))
tmmn_ds.air_temperature.attrs = OrderedDict(tmmn_attrs)
tmmn_ds.lat.attrs = OrderedDict(lat_attrs)
tmmn_ds.lon.attrs = OrderedDict(lon_attrs)
tmmn_ds.day.attrs = OrderedDict(day_attrs)
tmmn_ds.attrs = OrderedDict(globe_attrs)
tmmn_ds.to_netcdf('/raid3/mbapt/ecflow/pnw/met_files/tmmn.nc', mode='w', format='NETCDF4')
print("minimum temperature netcdf saved")

#maximum temperature
tmmx_ds = xr.open_dataset("http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/tmmx/tmmx_%s.nc?lon[0:1:1385],lat[0:1:584],day[%s:1:%s],air_temperature[%s:1:%s][0:1:1385][0:1:584]" %(year, num_day, num_day, num_day, num_day))
tmmx_ds.air_temperature.attrs = OrderedDict(tmmx_attrs)
tmmx_ds.lat.attrs = OrderedDict(lat_attrs)
tmmx_ds.lon.attrs = OrderedDict(lon_attrs)
tmmx_ds.day.attrs = OrderedDict(day_attrs)
tmmx_ds.attrs = OrderedDict(globe_attrs)
tmmx_ds.to_netcdf('/raid3/mbapt/ecflow/pnw/met_files/tmmx.nc', mode='w', format='NETCDF4')
print("maximum temperature netcdf saved")

#wind speed
vs_ds = xr.open_dataset("http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/vs/vs_%s.nc?lon[0:1:1385],lat[0:1:584],day[%s:1:%s],wind_speed[%s:1:%s][0:1:1385][0:1:584]" %(year, num_day, num_day, num_day, num_day))
vs_ds.wind_speed.attrs = OrderedDict(vs_attrs)
vs_ds.lat.attrs = OrderedDict(lat_attrs)
vs_ds.lon.attrs = OrderedDict(lon_attrs)
vs_ds.day.attrs = OrderedDict(day_attrs)
vs_ds.attrs = OrderedDict(globe_attrs)
vs_ds.to_netcdf('/raid3/mbapt/ecflow/pnw/met_files/vs.nc', mode='w', format='NETCDF4')
print("wind speed netcdf saved")

#replace start date, end date and year in the configuation file
replace(config_file, 'MODEL_START_DATE', date_format)
replace(config_file, 'MODEL_END_DATE', date_format)
