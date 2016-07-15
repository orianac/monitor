#!/usr/bin/env python
"""
swe_plot.py
usage: <python> <swe_plot.py> <configuration.cfg>

Plots the results of swe_analysis.py
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.io.shapereader as shpreader
from tonic.io import read_config
from monitor.plot import add_gridlines, add_map_features
import xarray as xr
import argparse
from datetime import datetime, timedelta
import os

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file',
                    type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in from configuration file
percent_file = config_dict['PLOT']['Percent_SWE']
plot_loc = config_dict['PLOT']['plot_SWE']
N = int(config_dict['ECFLOW']['Met_Delay'])

# how many days behind metdata is from realtime
date_unformat = datetime.now() - timedelta(days=N)

date = date_unformat.strftime('%Y-%m-%d')
date_ncfile = date_unformat.strftime('%Y%m%d')
month_day = date_unformat.strftime("%B_%-d")

# read in nc file with percentiles
dsx = xr.open_dataset(percent_file)

# plotting
plt.figure(figsize=(8, 8))

ax = plt.axes(projection=ccrs.Mercator(central_longitude=-120,
                                       min_latitude=40.7, max_latitude=49.3, globe=None))

ax.set_extent([-109.5, -125.01, 40, 54.01], ccrs.Geodetic())

gl = add_gridlines(ax)

add_map_features(ax)

cmap = matplotlib.colors.ListedColormap(['darkred', 'red', 'tomato', 'lightsalmon', 'mistyrose',
                                         'yellow', 'lightsteelblue', 'cornflowerblue',
                                         'royalblue', 'blue', 'navy'])

img = dsx['Percentile'].plot(ax=ax, vmin=0, vmax=10,
                             levels=[0, 0.01, 0.05, 0.1, 0.2, 0.35,
                                     0.65, 0.8, 0.9, 0.95, 0.99, 1.0],
                             add_colorbar=False, cmap=cmap, transform=ccrs.PlateCarree(), zorder=2)


plt.suptitle('SWE Percentile (threshold = 10mm)\n (%s)' % (date),
             y=1., x=.5, fontsize=16, fontweight='bold')

cbar = plt.colorbar(img, cmap=cmap, shrink=.95, orientation='horizontal')
labels = ['0', '1', '5', '10', '20', '35', '65', '80', '90', '95', '99', '100']
cbar.set_ticks([0, 0.01, 0.05, 0.1, 0.2, 0.35,
                0.65, 0.8, 0.9, 0.95, 0.99, 1.0])
cbar.set_ticklabels(labels)
cbar.ax.tick_params(labelsize=12)
cbar.ax.set_xlabel('percentile', fontsize=15)

# save figure
plt.savefig(os.path.join(plot_loc,'SWE_%s.png' % (date_ncfile)), bbox='tight')
