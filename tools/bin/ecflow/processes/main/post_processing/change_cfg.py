#!/usr/bin/env python

import datetime as dt
import argparse

from shutil import copyfile
from tonic.io import read_config
from monitor.io import replace


# parse arguments
parser = argparse.ArgumentParser(description='Run VIC')
parser.add_argument('config_file', metavar='config_file',
                        type=argparse.FileType('r'), nargs=1,
                        help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

input_dir = config_dict['VIC']['OutputDirRoot']
output_dir = config_dict['VIC2NC']['OutputDirNC']
startdate = config_dict['DATE']['Start_Date']
enddate = config_dict['DATE']['End_Date']
domain_file = config_dict['VIC2NC']['DomainFile']
config_file = config_dict['VIC2NC']['ConfigFile']
temp_config_file = config_dict['VIC2NC']['TempConfigFile']


copyfile(config_file, temp_config_file)

replace(temp_config_file, 'INPUT_DIR', input_dir)
replace(temp_config_file, 'OUTPUT_DIR', output_dir)
replace(temp_config_file, 'MODEL_START_DATE', startdate)
replace(temp_config_file, 'MODEL_END_DATE', enddate)
replace(temp_config_file, 'DOMAIN_FILE', domain_file)
