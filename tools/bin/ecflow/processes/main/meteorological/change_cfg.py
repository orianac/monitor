#!/usr/bin/env python

"""
change_cfg.py 
usage: <python> <change_cfg.py> <configuration.cfg>
Replaces the specified variables in the vic2nc config file. 
"""

import datetime as dt
import argparse

from shutil import copyfile
from tonic.io import read_config
from monitor import model_tools

# parse arguments
parser = argparse.ArgumentParser(description='Change cfg file')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

input_dir = config_dict['SUBDAILY']['Final_Subd_Dir']
output_dir = config_dict['SUBDAILY2NC']['OutputDirNC']
output_prefix = config_dict['SUBDAILY2NC']['OutputPrefix']
startdate = config_dict['SUBDAILY']['Subd_Met_Start_Date']
enddate = config_dict['SUBDAILY']['Subd_Met_End_Date']
domain_file = config_dict['SUBDAILY2NC']['DomainFile']
config_file = config_dict['SUBDAILY2NC']['ConfigFile']
temp_config_file = config_dict['SUBDAILY2NC']['TempConfigFile']


kwargs = {'INPUT_DIR': input_dir, 'OUTPUT_DIR': output_dir, 'OUTPUT_PREFIX': output_prefix,
          'MODEL_START_DATE': startdate, 'MODEL_END_DATE': enddate,
          'DOMAIN_FILE': domain_file}

model_tools.copy_clean_vic_config(
    config_file, temp_config_file, header=None, **kwargs)
