#!/usr/bin/env python
"""
This script uses multiprocessing to run VIC
usage: python run_vic.py configuation.cfg
"""


import argparse
import os
import types
import pandas as pd
import multiprocessing as mp
import subprocess
from datetime import datetime
from tempfile import mkstemp

from tonic.io import read_config
from tonic.models.vic import VIC
from monitor.log import set_logger
from monitor.share import LOG_LEVEL, _pickle_method
from monitor import model_tools
from monitor import os_tools
from monitor.compat import copy_reg

# -------------------------------------------------------------------------#


def main():
    """
    Run VIC for each ensemble member over given basin.
    """
    # parse arguments
    parser = argparse.ArgumentParser(description='Run VIC')
    parser.add_argument('config_file', metavar='config_file',
                        type=argparse.FileType('r'), nargs=1,
                        help='configuration file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file[0].name)
    # set up logger
    logger = set_logger(os.path.splitext(os.path.split(__file__)[-1])[0],
                        LOG_LEVEL)
    # update pickle method to work on instance method
    copy_reg.pickle(types.MethodType, _pickle_method)
    # read variables from config file
    cores = config_dict['ECFLOW']['Cores']
    global_file_template = config_dict['VIC']['GlobalFileTemplate']
    forcing_dir = config_dict['VIC']['ForcingDirRoot']
    output_dir = config_dict['VIC']['OutputDirRoot']
    log_dir_root = config_dict['VIC']['LogDirRoot']
    soil_root = config_dict['VIC']['SoilRoot']
    soil_list = config_dict['VIC']['SoilList']
    num_ens = config_dict['ECFLOW']['NEns']
    run_dir = config_dict['VIC']['RunDir']
    start_date = config_dict['DATE']['Start_Date']
    end_date = config_dict['DATE']['End_Date']
    executable = config_dict['VIC']['Executable']

    # set up vic to be run
    vic = VIC(executable)

    # parse out year, month, and day from start date
    model_start_year = start_date[:4]
    model_start_month = start_date[5:7]
    model_start_day = start_date[8:10]
    # parse out year, month, and day from end date
    model_end_year = end_date[:4]
    model_end_month = end_date[5:7]
    model_end_day = end_date[8:10]

    if cores > 1:
        pool = mp.Pool(processes=cores)
        logger.debug('started pool with {} processes'.format(cores))

    """
    if only reading in one soil lat lon list file, 
    then remove the first for loop 
    (log_dir, soillist will need to be changed)
    """

    global_file_list = []
    log_dir_list = []

    for ens in range(1, num_ens + 1):
        log_dir = '{0}{1:04d}'.format(log_dir_root, ens)
        os_tools.make_dirs(log_dir)
        # soil_ens = soil list file name
        soil_ens = "{0:04d}".format(ens)
        soillist = os.path.join(soil_list, 'pnw.%s' % (soil_ens))
        # soil file list
        ll_csv = pd.read_csv(soillist, sep=' ', header=None)
        latitude = ll_csv[0]
        longitude = ll_csv[1]

        # loop through each lat/lon to create new global parameter file for
        # each grid cell
        for lons, lats in zip(longitude, latitude):
            lat = str(lats)
            lon = str(lons)
            global_file_cell = os.path.join(
                run_dir, 'control_{0}_{1}'.format(lat, lon))
            kwargs = {'SOILROOT': soil_root, 'LATITUDE': lat, 'LONGITUDE': lon,
                      'OUTPUT_DIR': output_dir, 'FORCING_DIR': forcing_dir,
                      'START_YEAR': model_start_year, 'START_MONTH': model_start_month,
                      'START_DAY': model_start_day, 'END_YEAR': model_end_year,
                      'END_MONTH': model_end_month, 'END_DAY': model_end_day}
            model_tools.copy_clean_vic_config(global_file_template,
                                              global_file_cell, header=None, **kwargs)
            # creates a list of the pathways for each global parameter file to
            # be referenced by vic.run
            global_file_list.append(global_file_cell)
            log_dir_list.append(log_dir)
    # runs vic
    if cores > 1:
        try:
            logger.info('about to apply_async')
            retvals = [pool.apply_async(vic.run,
                                        args=(global_file_cell, log_dir)) for global_file_cell,
                       log_dir in zip(global_file_list, log_dir_list)]
            for r in retvals:
                       # return value should be 0 if VIC exited normally
                if r.get() != 0:
                    logger.critical(r.get())
        except (mp.ProcessError, mp.TimeoutError, mp.BufferTooShort,
                mp.AuthenticationError) as e:
            logger.info(e)
    else:
        retvals = [vic.run(global_file_cell, log_dir) for global_file_cell,
                   log_dir in zip(global_file_list, log_dir_list)]
        logger.info('run individually for {} cores'.format(cores))
        for r in retvals:
            if r != 0:
                logger.info(r)
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            os_tools.file_chmod(os.path.join(root, f))
        if cores > 1:
            pool.close()
            pool.join()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    copy_reg.pickle(types.MethodType, _pickle_method)
    main()
