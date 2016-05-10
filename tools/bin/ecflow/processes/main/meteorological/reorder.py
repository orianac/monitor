#!/usr/bin/env python
#####reorder.py
#####usage: <python> <reorder.py> <configuration.cfg>

#####In order for tonic (netcdf2vic.py) to read and convert these netcdf files
#####their dimensions must be day,lat,lon.
#####This script uses subprocess to execute the ncpdq command.

import os
import sys
import argparse
from tonic.io import read_config
from monitor.io import proc_subprocess

######### ----------------------------------------###########


#read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FileType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

#read in meterological data location
met_loc = config_dict['ECFLOW']['Met_Loc']

#netcdf file prefixes
param = ['pr', 'tmmn', 'tmmx', 'vs']

for var in param:
	#in file
	nc_file = '%s/%s.nc' %(met_loc, i)
	#out file
	reorder_file = '%s/%s.reorder.nc' %(met_loc, i)

	#remove previous days file, ncpdq doesn't overwrite
	if os.path.isfile(reorder_file):
		os.remove(reorder_file)

	reorder = ['ncpdq -a day,lat,lon %s %s' %(nc_file, reorder_file)]
	proc_subprocess(reorder, met_loc)
