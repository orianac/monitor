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
start_year = vic_start_date[:4]
start_month = vic_start_date[5:7]
start_day = vic_start_date[8:10]

end_year = vic_end_date[:4]
end_month = vic_end_date[5:7]
end_day = vic_end_date[8:10]

state_year = vic_save_state[:4]
state_month = vic_save_state[5:7]
state_day = vic_save_state[8:10]

in_state = os.path.join(state_path, 'state.%s%s%s_00000.nc' %
                        (start_year, start_month, start_day))

# replace the model year, month and day in the global file
kwargs = {'Start_Year': start_year, 'Start_Month': start_month, 'Start_Day': start_day,
          'End_Year': end_year, 'End_Month': end_month, 'End_Day': end_day,
          'State_Year': state_year, 'State_Month': state_month, 'State_Day': state_day,
          'In_State': in_state, 'State_Path': state_path}
model_tools.replace_var_pythonic_config(
    global_template, global_file_path, header=None, **kwargs)
