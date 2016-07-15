#!/usr/bin/env python
"""
combine.py
usage: <python> <combine.py> <configuration.cfg>
This script runs tocel_combine.py
"""
import numpy as np
import argparse
import os
import multiprocessing as mp
from shutil import copyfile

from tonic.io import read_config
from monitor.io import proc_subprocess
from monitor.log import set_logger
from monitor.share import LOG_LEVEL 
from monitor import model_tools
######### ----------------------------------------###########


def main():
    #parse arguments
    parser = argparse.ArgumentParser(description='Run VIC')
    parser.add_argument('config_file', metavar='config_file',
                    type=argparse.FileType('r'), nargs=1,
                    help='configuration file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file[0].name)

    #set up logger
    logger = set_logger(os.path.splitext(os.path.split(__file__)[-1])[0],
                    LOG_LEVEL)

    #read in variables from config file
    cores = config_dict['ECFLOW']['Cores']
    run_dir = config_dict['VIC']['RunDir']
    log_dir_root = config_dict['VIC']['LogDirRoot']
    forcing_names = config_dict['COMBINE']['Forcing_Name_List']
    script = config_dict['COMBINE']['Script'] #location of original tocel_combine.py 
    tmin_dir = config_dict['COMBINE']['Tmin_dir']
    tmax_dir = config_dict['COMBINE']['Tmax_dir']
    precip_dir = config_dict['COMBINE']['Precip_dir']
    wind_dir = config_dict['COMBINE']['Wind_dir']
    final_dir = config_dict['COMBINE']['Final_Met_dir']
	
    #set up multiprocessor
    if cores > 1:
    	pool = mp.Pool(processes=cores)
       	logger.debug('started pool with {} processes'.format(cores))

    #read in list of lat/lons
    lat_lon = np.genfromtxt(forcing_names, dtype='str')

    lat_lons_list=[]
    log_dir_list = []

    #in run_dir make a tocel_combine script for every grid cell 
    for ll in lat_lon:
    	log_dir = '{0}{1}'.format(log_dir_root, ll)
	lat_lons = os.path.join(
               	run_dir, 'control_{0}'.format(ll))
	copyfile(script, lat_lons)
	kwargs = {'DATA_LAT_LON': ll, 'TMIN_DIREC': tmin_dir, 
		'TMAX_DIREC': tmax_dir, 'PRECIP_DIREC': precip_dir, 
		'WIND_DIREC': wind_dir, 'FINAL_DIREC': final_dir}
	model_tools.copy_clean_vic_config(script,
                                     lat_lons, header=None, **kwargs)

	lat_lons_list.append(lat_lons)
	log_dir_list.append(log_dir)
	
    #run each tocel_combine script using subprocess
    if cores > 1:
	try:
	       	logger.info('about to apply_async')
          	for i in lat_lons_list:
               		run_tocel_combine = ['python', '%s' %(i)]
                 	pool.apply_async(proc_subprocess, args=(run_tocel_combine, run_dir))
	except (mp.ProcessError, mp.TimeoutError, mp.BufferTooShort,
        	         mp.AuthenticationError) as e:
             		logger.info(e)

    else:
 	logger.info('core set to 1')
	logger.info('about to apply_async')
        for i in lat_lons_list:
        	run_tocel_combine = ['python', 'i' ]

     		pool.apply_async(proc_subprocess, args=(run_tocel_combine, run_dir))
	
    #remove the tocel_combine scripts from run_dir		
    map(os.remove, lat_lons_list)

    #end multiprocessor
    if cores > 1:
    	pool.close()
       	pool.join()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()


