#!/usr/bin/env python
"""
prep_and_run_vic.py
usage: <python> <prep_and_run_vic.py> <configuration.cfg> <time_horizon>

This script creates a global file from the template with the correct
 model start and end dates.
"""
import argparse
import subprocess
import os
from datetime import timedelta
from dateutil.parser import parse

from tonic.io import read_config
from monitor import model_tools
import xarray as xr
import pandas as pd

def main():
    ''' Prepare global file from template and run VIC. Uses mpirun
        and executable defined in configuration file '''
    # read in configuration file
    parser = argparse.ArgumentParser(description='Run VIC')
    parser.add_argument('config_file', metavar='config_file',
                        help='configuration file')
    parser.add_argument('time_horizon_type', help='MONITOR, MED_FCST, or ' +
                        'SEAS_FCST. Should correspond to section header in ' +
                        'config_file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)
    section = args.time_horizon_type

    # read in the desired variables from the config file
    global_template = config_dict['DOMAIN']['GlobalFileTemplate']
    global_file_path = config_dict[section]['GlobalFilePath']
    # get important dates
    ds_seas = xr.open_dataset(config_dict[section]['Orig_Met'])
    vic_start_date = pd.to_datetime(ds_seas['time'].values[0]).strftime('%Y-%m-%d')
    vic_end_date = pd.to_datetime(ds_seas['time'].values[-1]).strftime('%Y-%m-%d')
    vic_save_state = (pd.to_datetime(ds_seas['time'].values[-1]) +
                      timedelta(days=1)).strftime('%Y-%m-%d')

    # but if you're doing the seasonal forecast you'll want the first date to
    # be the day after the end of the medium range forecast
    if section == 'SEAS_FCST':
        ds_med = xr.open_dataset(config_dict['MED_FCST']['Orig_Met'])
        vic_start_date = (pd.to_datetime(ds_med['time'].values[-1]) +
                      timedelta(days=1)).strftime('%Y-%m-%d')

    forcing_prefix = os.path.join(
        config_dict[section]['Subd_Out_Dir'],
        'forcing_{0}-{1}.'.format(vic_start_date.replace('-', ''),
                                  vic_end_date.replace('-', '')))

    # parse out year, month, and day from the model dates, which have
    # the form YYYY-MM-DD
    start = parse(vic_start_date)
    end = parse(vic_end_date)
    save_state = parse(vic_save_state)

    # generate the path to the initial state file
    in_state = os.path.join(config_dict[section]['InStatePath'],
                            'state.%s%s%s_00000.nc' % (
                                vic_start_date[:4], vic_start_date[5:7],
                                vic_start_date[8:10]))

    kwargs = {
        'Start_Year': start.year,
        'Start_Month': start.month,
        'Start_Day': start.day,
        'End_Year': end.year,
        'End_Month': end.month,
        'End_Day': end.day,
        'State_Year': save_state.year,
        'State_Month': save_state.month,
        'State_Day': save_state.day,
        'In_State': in_state,
        'State_Path': config_dict[section]['StatePath'],
        'Result_Path': config_dict[section]['OutputDirRoot'],
        'Forcing_Prefix': forcing_prefix}

    model_tools.replace_var_pythonic_config(
        global_template, global_file_path, header=None, **kwargs)

    # Use subprocess to submit the following command, with
    # mpirun executable, ncores, and vic executable read from
    # configuration file:
    # mpirun -np 16 vic_image.exe -g global_file
    subprocess.run([config_dict['ECFLOW']['MPIExec'], '-np',
                    str(config_dict['ECFLOW']['Cores']),
                    config_dict['ECFLOW']['Executable'], '-g',
                    global_file_path])


if __name__ == '__main__':
    main()
