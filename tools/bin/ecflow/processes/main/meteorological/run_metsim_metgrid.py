#!/usr/bin/env python
"""
create_metsim_cfg.py
usage: <python> <create_metsim_cfg.py> <configuration.cfg>
This script writes a MetSim configuration file and runs MetSim.
"""
import os
import argparse
import subprocess

from monitor import model_tools
from chunk_forcings import save_metsim_by_year
from tonic.io import read_config

# read in configuration file
parser = argparse.ArgumentParser(description='Create configuration file ' +
                                 'for MetSim')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

state = config_dict['SUBDAILY']['Met_State_File']
outstate = config_dict['SUBDAILY']['Fcst_Met_State']
forcing = config_dict['ECFLOW']['Orig_Met']
old_config_file = config_dict['SUBDAILY']['MetSim_Template']
new_config_file = config_dict['SUBDAILY']['MetSim_Cfg']
start_date = config_dict['SUBDAILY']['Subd_Met_Start_Date']
end_date = config_dict['SUBDAILY']['Subd_Met_End_Date']
out_dir = config_dict['SUBDAILY']['Subd_Out_Dir']

kwargs = {'STARTDATE': start_date.replace('-', '/'),
          'ENDDATE': end_date.replace('-', '/'),
          'FORCING': forcing, 'DOMAIN': config_dict['SUBDAILY']['GridFile'],
          'INSTATE': state, 'OUTDIR': out_dir, 'OUTSTATE': outstate}
model_tools.replace_var_pythonic_config(
    old_config_file, new_config_file, header=None, **kwargs)
print(os.environ['PATH'])
subprocess.check_call(['ms', new_config_file, '-n', '16'])

save_metsim_by_year(new_config_file)
