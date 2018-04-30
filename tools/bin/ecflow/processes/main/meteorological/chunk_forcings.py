''' chunk_forcings.py
usage: python save_metsim_by_year.py <metsim_config_file>
Reads in output from MetSim (corresponding to information in
metsim_config_file) and writes it to separate files by year for use in VIC '''
import os
import sys
import argparse
import numpy as np
import xarray as xr

from tonic.io import read_config


def save_metsim_by_year(metsim_config_file):
    ''' Reads MetSim output and writes it to separate files by calendar
        year so that they can be read to force VIC '''
    # read configuration file to get file name information
    config_dict = read_config(metsim_config_file)
    out_dir = config_dict['MetSim']['out_dir']
    out_prefix = config_dict['MetSim']['out_prefix']
    start = config_dict['MetSim']['start'].replace('/','')
    stop = config_dict['MetSim']['stop'].replace('/','')
    in_file = os.path.join(out_dir, '{0}_{1}-{2}.nc'.format(out_prefix,
                                                          start, stop))
    # read data and save by year
    if os.path.exists(in_file):
        xds =xr.open_dataset(in_file)
        for year in np.unique(xds.time.dt.year):
            out_file = os.path.join(out_dir, '{0}_{1}-{2}.{3}.nc'.format(
                out_prefix, start, stop, year))
            xds.sel(time=str(year)).to_netcdf(out_file)
    else:
        print('ERROR: {} does not exist!'.format(in_file))
        sys.exit(1)
    # TODO: delete in_file after writing out_file
    if os.path.exists(in_file) and os.path.exists(out_file):
        os.remove(in_file)

def main():
    ''' read in configuration file and call primary function '''
    parser = argparse.ArgumentParser(description='Save forcings by year')
    parser.add_argument('metsim_config_file', metavar='metsim_config_file',
                        help='MetSim configuration file')
    args = parser.parse_args()
    save_metsim_by_year(args.metsim_config_file)

if __name__ == "__main__":
    main()
