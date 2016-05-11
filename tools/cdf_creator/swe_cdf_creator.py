#!/usr/bin/env python
"""
swe_cdf_creator.py
usage: <python> <swe_cdf_creator.py> <configuration.cfg>

Creates list of ascending list from long-term SWE VIC produced data.
Period of time is defined in configuration file. 
Applies a moving window-esque method to collect 
5 days of data at a time and then sorts that data. 
February 29th data is removed before being sorted.
February 28th (plus surround two days) is stored at February 29th. 
"""
import datetime as dt
import numpy as np
import pandas as pd
import xray
import os
import time
import math
import gc
import argparse
from tonic.io import read_config

#read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', 
	type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

#read in meterological data location
direc = config_dict['CDF']['Historic_Direc']
f = config_dict['CDF']['Historic_File']
#number of plotting positions
num_pp = config_dict['CDF']['num_plot_pos']
start_date = config_dict['CDF']['Start_Date']
end_date = config_dict['CDF']['End_Date']
latlon_list = config_dict['CDF']['LatLon_List']
output_direc = config_dict['SWE']['Out_Dir']

num_pp = float(num_pp)

#load in netcdf file from which cdfs will be taken
ds = xray.open_dataset(os.path.join(direc,f))

#slice to the desired time period
ds_timeslice = ds.sel(time=slice(start_date, end_date))

#read in list of grid cells for the PNW
coordinates = pd.read_csv(latlon_list, sep=' ', 
	index_col=None, delimiter=None, header=None)

latitude = coordinates[0]
longitude = coordinates[1]

#create plotting positions using Wiebull distribution

q = []

for i in range(1,num_pp+1):
    q.append(i/(num_pp+1))

#cdfs for SWE
for i in range(0,len(latitude)):
    ds_latslice = ds_timeslice.sel(lat=latitude[i])
   
    #group by month and day
    s = ds_latslice.sel(lon=latitude[i]).to_series()
    #remove February 29th data
    ss = s[(s.index.month!=2) | (s.index.day!=29)]
    gb = ss.groupby([ss.index.month, ss.index.day])


    for g, data in gb:
        #get current day's data along with surrounding 2 days
        current_day = dt.datetime(year=2011, month=g[0], day=g[1])
        yesterday = current_day - dt.timedelta(days=1)
        tomorrow = current_day + dt.timedelta(days=1)
        yesterday_2 = current_day - dt.timedelta(days=2)
        tomorrow_2 = current_day + dt.timedelta(days=2)

        cc = pd.concat([gb.get_group(g), gb.get_group((yesterday.month, 
		yesterday.day)), gb.get_group((yesterday_2.month, 
		yesterday_2.day)), gb.get_group((tomorrow.month, 
		tomorrow.day)), gb.get_group((tomorrow_2.month, tomorrow_2.day))])
        
	cc.sort()

        month = current_day.strftime('%B')

        lat = latitude[i]
        lon = longitude[i]
        
	filename = '%s_%s' %(lat, lon)

        #save the 150 values by day and lat/lon
        month_dir = "%s_%s" %(month, current_day.day)
	path2 = os.path.join(output_direc, month_dir)
        if not os.path.exists(path2):
            os.makedirs(path2)
        savepath2 = os.path.join(path2, filename)
        cc.to_csv(savepath2, sep=' ', header=False, index=None)


#save February 28 data as February 29
for i in range(0,len(latitude)):
    lat_data = data_year.sel(lat=latitude[i])

    s = ds_latslice.sel(lon=latitude[i]).to_series()
    ss = s[(s.index.month!=2) | (s.index.day!=29)]
    gb = ss.groupby([ss.index.month, ss.index.day])


    g = (2,28)
    #get current day's data along with surrounding 2 days
    current_day = dt.datetime(year=2011, month=g[0], day=g[1])
    yesterday = current_day - dt.timedelta(days=1)
    tomorrow = current_day + dt.timedelta(days=1)
    yesterday_2 = current_day - dt.timedelta(days=2)
    tomorrow_2 = current_day + dt.timedelta(days=2)

    cc = pd.concat([gb.get_group(g), gb.get_group((yesterday.month, 
		yesterday.day)), gb.get_group((yesterday_2.month, 
		yesterday_2.day)), gb.get_group((tomorrow.month, 
		tomorrow.day)), gb.get_group((tomorrow_2.month, tomorrow_2.day))])
    
    cc.sort()

    month = current_day.strftime('%B')

    lat = latitude[i]
    lon = longitude[i]

    filename = '%s_%s' %(lat, lon)

    #save the 150 values by day and lat/lon
    month_dir = "%s_%s" %(month, current_day.day)
    path2 = os.path.join(output_direc, month_dir)
    if not os.path.exists(path2):
        os.makedirs(path2)
    savepath2 = os.path.join(path2, filename)
    cc.to_csv(savepath2, sep=' ', header=False, index=None)



