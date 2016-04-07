#!/usr/bin/env python
import argparse
import os
import types
import pandas as pd
import numpy as np
import multiprocessing as mp
import subprocess
from datetime import datetime
from tempfile import mkstemp
from shutil import move, rmtree

from tonic.io import read_config
from tonic.models.vic import VIC
from core.log import set_logger
from core.share import LOG_LEVEL, _pickle_method, copy_reg
from core import model_tools
from core import os_tools


# -------------------------------------------------------------------------#
### Function for replacing values, which will be useful for modifying the control files for the hydro models

def replace(file_path, pattern, subst):
    #Create temp file
    fh, abs_path = mkstemp()
    new_file = open(abs_path,'w')
    os.chmod(abs_path,0755)
    old_file = open(file_path)
    for line in old_file:
        new_file.write(line.replace(pattern, subst))
    #close temp file
    new_file.close()
    os.close(fh)
    old_file.close()
    #Remove original file
    os.remove(file_path)
    #Move new file
    move(abs_path, file_path)

#### define function that will execute command line shell script operations

def proc_subprocess(executing_arguments, dir):
    proc = subprocess.Popen( ' '.join(executing_arguments),
                            shell=True,
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE )
    retvals = proc.communicate()

    stdout = retvals[0]
    stderr = retvals[1]
    returncode = proc.returncode

    with open(dir+'log_file', "a" ) as logfile:
        logfile.write( stderr+stdout )

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
    #output_dir_root = config_dict['VIC']['OutputDirRoot']
    output_dir = config_dict['VIC']['OutputDirRoot']
    #state_file_root = config_dict['VIC']['StateFileRoot']
    log_dir_root = config_dict['VIC']['LogDirRoot']
    soil_root = config_dict['VIC']['SoilRoot']
    soil_list = config_dict['VIC']['SoilList']
    num_ens = config_dict['ECFLOW']['NEns']
    #executable = config_dict['VIC']['Exec']
    run_dir = config_dict['VIC']['RunDir']
    date_file = config_dict['VIC']['DateFile']
	
    # Initialize VIC
    #vic = VIC(executable)
    
    #parse out year, month, and day from j_ab_end_date
    end_date = np.genfromtxt('%s' %(date_file), dtype='str', delimiter='-', skip_header=0)
    end_date_year = end_date[0]
    end_date_month = end_date[1]
    end_date_day = end_date[2]

    #definitely need to change this part		
    #ll_df = pd.read_csv(soil_list, sep=' ', skipinitialspace=True,
    #                    usecols=[0, 2, 3], names=['flag', 'lat', 'lon'],
    #                    dtype={'flag': int, 'lat': str, 'lon': str},
    #                    header=None)
    if cores > 1:
        pool = mp.Pool(processes=cores)
        logger.debug('started pool with {} processes'.format(cores))
    global_file_list = []
    log_dir_list = []
    for ens in range(1, num_ens + 1):
        #forcing_dir = '{0}{1:03d}'.format(forcing_dir_root, ens)
        log_dir = '{0}{1:04d}'.format(log_dir_root, ens)
        os_tools.make_dirs(log_dir)
        if not os.path.exists(forcing_dir):
            logger.critical('Could not find forcing directory: {0}'.format(
                forcing_dir))
        #output_dir = '{0}{1:04d}'.format(output_dir_root, ens)
        #os_tools.make_dirs(output_dir)
	soil_ens = "{0:0=4d}".format(ens)
        soillist = '{0}/pnw.{1}'.format(soil_list,soil_ens)
        ll_csv = pd.read_csv(soillist, sep=' ', header=None)
	latitude = ll_csv[0]
	longitude = ll_csv[1]
	x = int(len(latitude))	

	for i in range(0, x):
	    lat=str(latitude[i])
	    lon=str(longitude[i])	
        #for _, _, lat, lon in ll_df.where(ll_df['flag'] == 1).itertuples():
            global_file_cell = os.path.join(
                run_dir, 'control_{0}_{1}'.format(lat, lon))
	#state_file = '{0}{1}_{2}_{3}'.format(state_file_root, lat, lon,  ens)
            
	    #kwargs = {'END_YEAR': end_date_year, 'END_MONTH': end_date_month, 'END_DAY': end_date_day, 'SOILROOT': soil_root, 'LATITUDE': lat, 'LONGITUDE': lon, 'OUTPUT_DIR': output_dir, 'FORCING_DIR': forcing_dir}
           # if config_dict['VIC']['RealTime']:
           #     now = datetime.now()
           #     kwargs['RTYEAR'] = str(now.year)
           #     kwargs['RTMONTH'] = str(now.month)
           #     kwargs['RTDAY'] = str(now.day)
            #model_tools.copy_clean_vic_config(global_file_template, global_file_cell, header=None, **kwargs)
            #print(lat)
	    model_tools.copy_clean_vic_config(global_file_template, global_file_cell, header=None)
            replace(global_file_cell, 'SOILROOT', soil_root)
	    replace(global_file_cell, 'LATITUDE', lat)
	    replace(global_file_cell, 'LONGITUDE', lon)
	    replace(global_file_cell, 'OUTPUT_DIR', output_dir)
	    replace(global_file_cell, 'FORCING_DIR', forcing_dir)
	    replace(global_file_cell, 'END_YEAR', end_date_year)
	    replace(global_file_cell, 'END_MONTH', end_date_month)
	    replace(global_file_cell, 'END_DAY', end_date_day)

            global_file_list.append(global_file_cell)
            log_dir_list.append(log_dir)
    #print(log_dir_list)
    #print(global_file_list)		
    #exit()
    if cores > 1:
        try:
            logger.info('about to apply_async')
            #run_model_args = ['/home/mbapt/VIC/src/vicNl', '-g' ]
            for i in global_file_list:
                 run_model_args = ['/home/mbapt/VIC/src/vicNl', '-g' ]
                 run_model_args.append(i)
                 #print(run_model_args)
                 #proc_subprocess(run_model_args, run_dir)
		 pool.apply_async(proc_subprocess, args=(run_model_args, run_dir))

                 #print(i)

            #retvals = [pool.apply_async(vic.run,
            #           args=(global_file_cell, log_dir)) for global_file_cell,
            #           log_dir in zip(global_file_list, log_dir_list)]
	    	
            
            #for r in retvals:
                # return value should be 0 if VIC exited normally
             #   if r.get() != 0:
             #       logger.critical(r.get())
	#	    print(global_file_cell)	
        except (mp.ProcessError, mp.TimeoutError, mp.BufferTooShort,
                mp.AuthenticationError) as e:
            logger.info(e)
    
    else:
	print('did not work')
        #exit()
        #retvals = [vic.run(global_file_cell, log_dir) for global_file_cell,
        #           log_dir in zip(global_file_list, log_dir_list)]
        #logger.info('run individually for {} cores'.format(cores))
        #for r in retvals:
        #    if r != 0:
        #        logger.info(r)
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
