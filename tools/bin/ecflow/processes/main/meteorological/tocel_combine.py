#!/usr/bin/env python


import pandas as pd
import numpy as np
import argparse

from tonic.io import read_config

# parse arguments
parser = argparse.ArgumentParser(description='Run VIC')
parser.add_argument('config_file', metavar='config_file',
                        type=argparse.FileType('r'), nargs=1,
                        help='configuration file')
args = parser.parse_args()
config_dict = read_config(args.config_file[0].name)

forcing_names = config_dict['COMBINE']['Forcing_Names']
tmin_kel_dir = config_dict['COMBINE']['Tmin_Kelvin_dir']
tmax_kel_dir = config_dict['COMBINE']['Tmax_Kelvin_dir']
tmin_cel_dir = config_dict['COMBINE']['Tmin_Cel_dir']
tmax_cel_dir = config_dict['COMBINE']['Tmax_Cel_dir']
precip_dir = config_dict['COMBINE']['Precip_dir']
wind_dir = config_dict['COMBINE']['Wind_dir']
final_dir = config_dict['COMBINE']['Final_Met_dir']


#read in list of lat/lons
#lat_lon = np.genfromtxt('%s' %(forcing_names), dtype='str')

#iterate through lat/lons to change temperature units from kelvin to celcius and then save

tmin_kel = np.genfromtxt('%s/%s' %(tmin_kel_dir, DATA_LAT_LON), dtype='float')
tmax_kel = np.genfromtxt('%s/%s' %(tmax_kel_dir, DATA_LAT_LON), dtype='float')

tmin_cel = tmin_kel - 273.15
tmax_cel = tmax_kel - 273.15

np.savetxt('%s/%s' %(tmin_cel_dir, DATA_LAT_LON), tmin_cel, fmt='%f')
np.savetxt('%s/%s' %(tmax_cel_dir, DATA_LAT_LON), tmax_cel, fmt='%f')

#columns are based on vic forcings input

precip = np.genfromtxt('%s/%s' %(precip_dir, DATA_LAT_LON))
tmax = np.genfromtxt('%s/%s' %(tmax_cel_dir, DATA_LAT_LON))
tmin = np.genfromtxt('%s/%s' %(tmin_cel_dir, DATA_LAT_LON))
wind = np.genfromtxt('%s/%s' %(wind_dir, DATA_LAT_LON))



d = {'precipitation': precip, 'tmax': tmax, 'tmin': tmin, 'wind': wind}
df = pd.DataFrame(data=d)

df.to_csv('%s/%s' %(final_dir, DATA_LAT_LON), sep='\t', header=False, index=False)
