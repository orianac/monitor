#!/usr/bin/env python

import os
import sys
from tonic.io import read_config
from core.io import proc_subprocess

met_loc = sys.argv[2]

parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file', type=argparse.FielType('r'), nargs=1, help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

year = config_dict['DATE']['Year']

param = ['pr', 'tmmn', 'tmmx', 'vs']

os.chdir(met_loc)

for i in param:
	try:
		os.remove('%s_%s.nc' %(i, year))

		reorder = ['ncpdq -a day,lat,lon %s_%s.nc %s_%s.reorder.nc' %(i, year, i, year)]
		proc_subprocess(reorder, met_loc)
	except FileNotFoundError:
		reorder = ['ncpdq -a day,lat,lon %s_%s.nc %s_%s.reorder.nc' %(i, year, i, year)]
                proc_subprocess(reorder, met_loc)
