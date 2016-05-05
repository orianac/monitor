#!/usr/bin/env python
###### plotting swe percentiles
###### usage: <python> <swe_plot.py> <configuration.cfg>

###### Creates a SWE percential spatial plot 
###### Compares one day of VIC output to 30 years of data
###### Using the Weibull Plotting Position

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import xarray as xr
import os
import time
import math
import gc
import argparse
from netCDF4 import Dataset
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from matplotlib.colors import ListedColormap
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.io.shapereader as shpreader
from tonic.io import read_config



#read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

#read in from configuration file
direc = config_dict['VIC2NC']['OutputDirNC']
N = config_dict['ECFLOW']['Met_Delay']
cdf_loc = config_dict['PLOT']['cdf_SWE']
plot_loc = config_dict['PLOT']['plot_SWE']

N = int(N)
#how many days behind metdata is from realtime

date_unformat = datetime.now() - timedelta(days=N)

date = date_unformat.strftime('%Y-%m-%d')
date_ncfile = date_unformat.strftime('%Y%m%d')
month_day = date_unformat.strftime("%B_%-d")



#read VIC output that has been converted to a netcdf file 
file = "pnw_daily_run.%s-%s.nc" %(date_ncfile, date_ncfile)

ds = xr.open_dataset(os.path.join(direc,file))


#create list of all latitudes and longitudes for full rectangle
un_lat = ds['lat'].values
un_lon = ds['lon'].values

num_lat = len(un_lat)
num_lon = len(un_lon)

latitude = np.repeat(un_lat,num_lon)
longitude = np.tile(un_lon, num_lat)


#Weibull Plotting Position
q = []

for i in range(1,151):
    q.append(i/(150.0+1))
   

min_q = min(q)/2
max_q = max(q)+min_q


#select out time and variable data 
ds_day = ds.sel(time=date)
swe_ds = ds_day['SWE']


#create a dictionary containing the lat, lon and corresponding percentile value (if one exists)
d = []

for i in range(0,len(latitude)):
    
    #iterate through all latitudes and longitudes
    ds_lat = swe_ds.sel(lat=latitude[i])
    ds_lon = ds_lat.sel(lon=longitude[i])
    
    value = ds_lon.values

    lat = latitude[i]
    lon = longitude[i]
    #read in sorted list of cdf values  
    #if cdf cannot be read then that lat lon is saved in the dictionary without a percentile
    #this is done to make creating the xarray dataset easier later on
    try: 
	cdf = pd.read_csv('%s/%s/%s_%s' %(cdf_loc, month_day, lat, lon), 
                          index_col=None, delimiter=None, header=None)
        x = cdf[0]
        
	#10mm threshold
        if (value < 10):
	    combine = (lat, lon)
            d.append(combine)
        else:    
        
            try:
                #interpolation
                f = interp1d(x,q)
                f(value)
                percentile = f(value)
                combine = (lat, lon, float(percentile))
                d.append(combine)
            #if interpolation fails then a value is assigned based on whether it is higher or lower than the range
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

#Dictionary to DataFrame to Dataset
df = pd.DataFrame(d, columns=["Latitude", "Longitude", "Percentile"])
a = df['Percentile'].values
new = a.reshape(195, 245)

dsx = xr.Dataset({'percentile': (['lat', 'lon'], new)}, coords={'lon': (['lon'], un_lon), 'lat': (['lat'], un_lat)})

#plotting
plt.figure(figsize=(8,8))

ax = plt.axes(projection=ccrs.Mercator(central_longitude=-120, min_latitude=40.7, max_latitude=49.3, globe=None)) 

ax.set_extent([-109.5, -125.01, 40, 54.01],ccrs.Geodetic())

gl=ax.gridlines(draw_labels=True, xlocs = [-100, -110, -115, -120, -125], ylocs = [40, 42, 45, 48, 50])
gl.xlabels_top = False
gl.ylabels_right = False

gl.xformatter = LONGITUDE_FORMATTER
gl.yformatter = LATITUDE_FORMATTER

states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

country_borders = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_0_boundary_lines_land',
        scale='50m',
        facecolor='none')

land = cfeature.NaturalEarthFeature(
    category='physical',
    name='land',
    scale='50m',
    facecolor='gray')

ocean = cfeature.NaturalEarthFeature(
    category='physical',
    name='ocean',
    scale='50m',
    facecolor='blue')

ax.add_feature(states_provinces, edgecolor='black', zorder = 2)
ax.add_feature(country_borders, edgecolor='black', zorder = 2)
ax.add_feature(land,facecolor='gray', zorder = 1)
ax.add_feature(ocean,facecolor='lightblue', zorder = 1)


cmap = matplotlib.colors.ListedColormap(['darkred', 'red', 'tomato', 'lightsalmon', 'mistyrose', 
                                         'yellow', 'lightsteelblue', 'cornflowerblue', 'royalblue', 'blue', 'navy'])
cmap.set_bad('lightgrey')
img=dsx['percentile'].plot(ax = ax, vmin = 0, vmax = 10, levels=[0, 0.01, 0.05, 0.1, 0.2, 0.35, 0.65, 0.8, 0.9, 0.95, 0.99, 1.0], add_colorbar=False, cmap=cmap, transform = ccrs.PlateCarree(), zorder=2)


plt.suptitle('SWE Percentile (threshold = 10mm)\n (%s)'%(date), y=1., x=.5, fontsize=16, fontweight='bold')     

cbar = plt.colorbar(img, cmap = cmap, shrink=.95, orientation='horizontal')
labels = ['0','1', '5', '10', '20', '35', '65', '80', '90', '95', '99', '100']
cbar.set_ticks([0, 0.01, 0.05, 0.1, 0.2, 0.35, 0.65, 0.8, 0.9, 0.95, 0.99, 1.0])
cbar.set_ticklabels(labels)
cbar.ax.tick_params(labelsize=12)
cbar.ax.set_xlabel('percentile', fontsize=15)

#save figure
plt.savefig('%s/SWE_%s' %(plot_loc, date_ncfile), format='png', bbox='tight')



