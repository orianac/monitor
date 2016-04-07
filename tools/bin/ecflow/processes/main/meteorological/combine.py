#!/usr/bin/env python

###### This script changes kelvin to celcius for the temperature parameters
###### Checks that the data for all the parameters is the same length
###### If it is not, then it corrects the lengths.
###### Combines the parameters into 1 file for each grid cell. 
import sys
import pandas as pd
import numpy as np
import argparse
import os
import multiprocessing as mp
import shutil import copyfile

from core.io import replace, proc_subprocess
from core.log import set_logger
from core.share import LOG_LEVEL, _pickle_method, copy_reg
from core import model_tools
from core import os_tools


#############_______________###########

from tonic.io import read_config

# parse arguments
parser = argparse.ArgumentParser(description='Run VIC')
parser.add_argument('config_file', metavar='config_file',
                        type=argparse.FileType('r'), nargs=1,
                        help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

forcing_names = config_dict['COMBINE']['Forcing_Names']
run_dir = config_dict['VIC']['RunDir']
cores = config_dict['ECFLOW']['Cores']
script = config_dict['COMBINE']['Script'] #location of original script

if cores > 1:
        pool = mp.Pool(processes=cores)
        logger.debug('started pool with {} processes'.format(cores))
#read in list of lat/lons
lat_lon = np.genfromtxt('%s' %(forcing_names), dtype='str')

lat_lons=[]
lat_lons_list=[]
for ll in lat_lon:

	lat_lons = os.path.join(
                run_dir, 'control_{0}'.format(ll))
	copyfile(script, lat_lons)
	replace(lat_lons, 'DATA_LAT_LON', ll)
	lat_lons_list.append(lat_lons)

if cores > 1:
        try:
            logger.info('about to apply_async')
            #run_model_args = ['/home/mbapt/VIC/src/vicNl', '-g' ]
            for i in lat_lons_list:
                 run_tocel_combine = ['python', 'i' ]
                 
                 pool.apply_async(proc_subprocess, args=(run_tocel_combine, run_dir))

        except (mp.ProcessError, mp.TimeoutError, mp.BufferTooShort,
                mp.AuthenticationError) as e:
            logger.info(e)

    else:
        print('did not work')
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            os_tools.file_chmod(os.path.join(root, f))
    if cores > 1:
        pool.close()
        pool.join()





