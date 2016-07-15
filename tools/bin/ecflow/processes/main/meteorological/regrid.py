#!/usr/bin/env python
"""
regrid.py
usage: <python> <regrid.py> <configuration.cfg>

This script changes the grid and domain in accordance with 
the grid_file, using remapcon.
Remapcon was selected to ensure that no precipiation was lost
in the regridding process. 
"""
import os
import sys
import argparse
from cdo import Cdo
cdo = Cdo()
from tonic.io import read_config

######### ----------------------------------------###########

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file',
                    type=argparse.FileType('r'), nargs=1,
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in met location from config file
met_loc = config_dict['ECFLOW']['Met_Loc']

# read in grid_file from config file
grid_file = os.path.join(met_loc, 'grid_info')

# netcdf file prefixes
param = ['pr', 'tmmn', 'tmmx', 'vs']


for var in param:
    # in file
    reorder_file = os.path.join(met_loc, '%s.reorder.nc' % (var))
    # out file
    regrid_file = os.path.join(met_loc, '%s.regrid.nc' % (var))

    # remove previous days file, cdo doesn't overwrite
    if os.path.isfile(regrid_file):
        os.remove(regrid_file)

    cdo.remapcon(grid_file, input=reorder_file, output=regrid_file)
