#!/usr/bin/env python
"""
create_global_file.py
usage: <python> <create_global_file.py> <configuration.cfg>

This script creates a global file from the template with the correct
 model start and end dates.
It also slices the subdaily met data to the correct date.
"""
import argparse
from tonic.io import read_config
from monitor import model_tools

######### ----------------------------------------###########

# read in configuration file
parser = argparse.ArgumentParser(description='Reorder dimensions')
parser.add_argument('config_file', metavar='config_file',
                    type=argparse.FileType('r'), nargs=1,
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

# read in the desired variables from the config file
global_template = config_dict['VIC']['GlobalFileTemplate']
global_file_path = config_dict['VIC']['GlobalFilePath']
model_date = config_dict['VIC']['ModelDate']

# parse out year, month, and day from the model date
model_year = model_date[:4]
model_month = model_date[5:7]
model_day = model_date[8:10]

# replace the model year, month and day in the global file
kwargs = {'Model_Year': model_year,
          'Model_Month': model_month, 'Model_Day': model_day}
model_tools.replace_var_pythonic_config(
    global_template, global_file_path, header=None, **kwargs)
