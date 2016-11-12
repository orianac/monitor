#!/usr/bin/env python
"""
transferring percentile files
usage: <python> <transfer_files.py> <configuration.cfg>

Uses paramiko to transfer percentile netCDFs to the
NKN network.
"""
import os
import paramiko
import argparse
from tonic.io import read_config

# read in configuration file
parser = argparse.ArgumentParser(description='Download met data')
parser.add_argument('config_file', metavar='config_file',
                    help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file)

# read in the source and destination paths and current date
source_loc = config_dict['PERCENTILES']['Percentile_Loc']
dest_loc = config_dict['PERCENTILES']['Percentile_Dest']
date = config_dict['VIC']['vic_end_date']

ssh = paramiko.SSHClient()
ssh.load_host_keys(os.path.expanduser(
    os.path.join("~", ".ssh", "known_hosts")))
ssh.connect('reacchdb.nkn.uidaho.edu', username='vicmet', password='cl!m@te')
sftp = ssh.open_sftp()

vars = ['swe', 'sm', 'tm']

for var in vars:
    sftp.put(os.path.join(source_loc, 'vic-metdata_%spercentile_%s.nc' %
                          (var, date)), os.path.join(dest_loc, 'vic-metdata_%spercentile.nc' % (var)))

sftp.close()
ssh.close()
