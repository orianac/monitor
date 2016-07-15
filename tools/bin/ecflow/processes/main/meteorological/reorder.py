#!/usr/bin/env python
"""
reorder.py
usage: <python> <reorder.py> <configuration.cfg>

In order for tonic (netcdf2vic.py) to read and convert these netcdf files
their dimensions must be day,lat,lon.
This script uses subprocess to execute the ncpdq command.
"""
import os
import sys
from nco import Nco
nco = Nco()
import argparse
from tonic.io import read_config
from monitor.io import proc_subprocess

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FileType(
    'r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in meterological data location
met_loc = config_dict['ECFLOW']['Met_Loc']

# netcdf file prefixes
param = ['pr', 'tmmn', 'tmmx', 'vs']

for var in param:
    # in file
    nc_file = os.path.join(met_loc, '%s.nc' % (var))
    # out file
    reorder_file = os.path.join(met_loc, '%s.reorder.nc' % (var))

    # remove previous days file, ncpdq doesn't overwrite
    if os.path.isfile(reorder_file):
        os.remove(reorder_file)

    nco.ncpdq(input=nc_file, output=reorder_file, arrange='day,lat,lon')
