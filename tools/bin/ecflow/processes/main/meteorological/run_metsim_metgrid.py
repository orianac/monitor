#!/usr/bin/env python
"""
create_metsim_cfg.py
usage: <python> <create_metsim_cfg.py> <configuration.cfg>
This script writes a MetSim configuration file and runs MetSim.
"""
import os
import argparse
import subprocess
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
import numpy as np
from monitor import model_tools
from chunk_forcings import save_metsim_by_year
from tonic.io import read_config

# read in configuration file
parser = argparse.ArgumentParser(description='Create configuration file ' +
                                 'for MetSim')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
parser.add_argument('time_horizon_type', help='MONITOR, MED_FCST, or ' +
                    'SEAS_FCST. Should correspond to section header in ' +
                    'config_file')
args = parser.parse_args()
config_dict = read_config(args.config_file)
section = args.time_horizon_type

state = config_dict[section]['Met_State_File']
outstate = config_dict[section]['Out_Met_State']
forcing = config_dict[section]['Orig_Met']
old_config_file = config_dict[section]['MetSim_Template']
new_config_file = config_dict[section]['MetSim_Cfg']
start_date = config_dict[section]['Start_Date']
end_date = config_dict[section]['End_Date']
out_dir = config_dict[section]['Subd_Out_Dir']


if section == 'MED_FCST':
    model = 'CFSv2'
    ensemble_members = ['00_1', '00_2', '00_3', '00_4',
                        '06_1', '06_2', '06_3', '06_4',
                        '12_1', '12_2', '12_3', '12_4',
                        '18_1', '18_2', '18_3', '18_4']
    med_fcst_ds = xr.open_dataset(forcing)
    #med_fcst_ds = xr.open_dataset('/civil/hydro/climate_toolbox/met_data/med_fcst/us/CFSv2.00_1.2019-08-08.nc')
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
    start_date = pd.to_datetime(med_fcst_ds['time'].values[0]).strftime('%Y-%m-%d')
    end_date = pd.to_datetime(med_fcst_ds['time'].values[-1]).strftime('%Y-%m-%d')
elif section == 'SEAS_FCST':
    # Eventually, we might want to run multiple ensemble members. Dates could still be
    # read from the just one file but forcing = config_dict[section]['Orig_Met'] will
    # probably need to change
    model = 'CFSv2'
    med_fcst_ds = xr.open_dataset(config_dict['MED_FCST']['Orig_Met'])
    # Overwrite start_date to be one day after the final date of the medium range forecast.
    # Overwrite end_date to be the final date of the seasonal range forecast.
    start_date = (pd.to_datetime(med_fcst_ds['time'].values[-1]) +
                  timedelta(days=1)).strftime('%Y-%m-%d')
    model = 'ENSMEAN'
    seas_fcst_ds = xr.open_dataset(config_dict['SEAS_FCST']['Orig_Met'])
    end_date = pd.to_datetime(seas_fcst_ds['time'].values[-1]).strftime('%Y-%m-%d')
    # Make sure tmax >= tmin always
#    tmin = np.copy(seas_fcst_ds['tasmin'].values)
#    tmax = np.copy(seas_fcst_ds['tasmax'].values)
#    swap_values = ((tmin > tmax) & (tmax != -32767.))
#    seas_fcst_ds['tasmin'].values[swap_values] = tmax[swap_values]
#    seas_fcst_ds['tasmax'].values[swap_values] = tmin[swap_values]
#    seas_fcst_ds.to_netcdf(os.path.join(seas_met_fcst_loc, '%s.nc.new' % (model)))
kwargs = {'STARTDATE': start_date.replace('-', '/'),
          'ENDDATE': end_date.replace('-', '/'),
          'FORCING': forcing, 'DOMAIN': config_dict['DOMAIN']['GridFile'],
          'INSTATE': state, 'OUTDIR': out_dir, 'OUTSTATE': outstate}
if section == 'MED_FCST':
    for ensemble_member in ensemble_members:
        forcing_path = os.path.dirname(config_dict['MED_FCST']['Met_Loc'])
        kwargs['FORCING'] = forcing_path+'/us/'+ensemble_member+'/'+model+'.'+ensemble_member+'.'+start_date+'.nc'
        kwargs['OUTDIR'] = out_dir + '/' + ensemble_member + '/'
        os.makedirs(kwargs['OUTDIR'], exist_ok=True)
        model_tools.replace_var_pythonic_config(
            old_config_file, new_config_file, header=None, **kwargs)
        subprocess.check_call(['ms', new_config_file, '-n', '15'])
        save_metsim_by_year(new_config_file)
else:
    model_tools.replace_var_pythonic_config(
        old_config_file, new_config_file, header=None, **kwargs)
    print(os.environ['PATH'])
    subprocess.check_call(['ms', new_config_file, '-n', '15'])

    save_metsim_by_year(new_config_file)
    os.rename(outstate, outstate[:-3]+'.'+str(end_date)+'.nc')
