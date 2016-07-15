'''
plot.py
'''

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.io.shapereader as shpreader


def add_gridlines(axis):
    gl = axis.gridlines(draw_labels=True,
                        xlocs=[-100, -110, -115, -120, -125],
                        ylocs=[40, 42, 45, 48, 50, 52, 54])
    gl.xlabels_top = False
    gl.ylabels_right = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    return gl


def add_map_features(axis, states_provinces=True, country_borders=True,
                     land=True, ocean=True):
    if states_provinces:
        states_provinces = cfeature.NaturalEarthFeature(
            category='cultural',
            name='admin_1_states_provinces_lines',
            scale='10m',
            facecolor='none')
        axis.add_feature(states_provinces, edgecolor='black', zorder=2)
    if country_borders:
        country_borders = cfeature.NaturalEarthFeature(
            category='cultural',
            name='admin_0_boundary_lines_land',
            scale='50m',
            facecolor='none')
        axis.add_feature(country_borders, edgecolor='black', zorder=2)
    if land:
        land = cfeature.NaturalEarthFeature(
            category='physical',
            name='land',
            scale='50m',
            facecolor='gray')
        axis.add_feature(land, facecolor='gray', zorder=1)
    if ocean:
        ocean = cfeature.NaturalEarthFeature(
            category='physical',
            name='ocean',
            scale='50m',
            facecolor='blue')
        axis.add_feature(ocean, facecolor='lightblue', zorder=1)
