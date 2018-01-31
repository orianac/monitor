#!/usr/bin/env python
"""
create_global_file.py
usage: <python> <create_global_file.py> <configuration.cfg>

This script creates a global file from the template with the correct
 model start and end dates.
"""
import argparse
from tonic.io import read_config
from monitor import model_tools
import os
from dateutil.parser import parse
######### ----------------------------------------###########

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

# read in the desired variables from the config file
global_template = config_dict['VIC']['GlobalFileTemplate']
global_file_path = config_dict['VIC']['GlobalFilePath']
state_path = config_dict['VIC']['StatePath']

vic_start_date = config_dict['VIC']['vic_start_date']
vic_end_date = config_dict['VIC']['vic_end_date']
vic_save_state = config_dict['VIC']['vic_save_state']

# parse out year, month, and day from the model dates, which have
# the form YYYY-MM-DD
start = parse(vic_start_date)
end = parse(vic_end_date)
save_state = parse(vic_save_state)

# generate the path to the initial state file
in_state = os.path.join(state_path, 'state.%s%s%s_00000.nc' % (
    vic_start_date[:4], vic_start_date[5:7], vic_start_date[8:10]))

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
    'State_Path': state_path}


model_tools.replace_var_pythonic_config(
    global_template, global_file_path, header=None, **kwargs)
