#!/usr/bin/env python
"""
tocel_combine.py

combine.py runs this script
by replacing the ALL CAPITAL words
and using subprocess.
This script changes tmin and tmax units from Kelvin to Celsius
and combines precip, tmax, tmin, and wind into 1 file for each grid cell. 
"""
import numpy as np
import os
from monitor.share import KELVIN

# these paths will be filled in by combine.py
tmin_dir = '{TMIN_DIREC}'
tmax_dir = '{TMAX_DIREC}'
precip_dir = '{PRECIP_DIREC}'
wind_dir = '{WIND_DIREC}'
srad_dir = '{SRAD_DIREC}'
sph_dir = '{SPH_DIREC}'
final_dir = '{FINAL_DIREC}'
full_year = '{FULL_YEAR}'

# read in tmin and tmax
# temperature data is in Kelvin, switch to Celcius
tmin_kel = np.genfromtxt(os.path.join(tmin_dir, '{DATA_LAT_LON}'))
tmax_kel = np.genfromtxt(os.path.join(tmax_dir, '{DATA_LAT_LON}'))

tmin_cel = tmin_kel - KELVIN
tmax_cel = tmax_kel - KELVIN

# read in precip, wind, shortwave radiation and specific humidity
precip = np.genfromtxt(os.path.join(
    precip_dir, '{DATA_LAT_LON}'), dtype='float')
wind = np.genfromtxt(os.path.join(wind_dir, '{DATA_LAT_LON}'), dtype='float')
srad = np.genfromtxt(os.path.join(srad_dir, '{DATA_LAT_LON}'), dtype='float')
sph = np.genfromtxt(os.path.join(sph_dir, '{DATA_LAT_LON}'), dtype='float')

combined = np.column_stack((precip, tmax_cel, tmin_cel, wind, srad, sph))

if full_year == 'Year':
    # if we downloaded the whole year, we simply need to save the data
    np.savetxt(os.path.join(
        final_dir, '{DATA_LAT_LON}'), combined, delimiter='  ', fmt="%.5f")

else:  # if we only downloaded the last 60 days we need to append it to the existing data
    # read in the existing data
    existing_data = np.genfromtxt(os.path.join(
        final_dir, '{DATA_LAT_LON}'), dtype='float')
    # remove the last 60 days
    existing_data = existing_data[0:len(existing_data) - 60]
    # remove the first day
    existing_data = existing_data[1:len(existing_data)]
    # append newest met data
    output = np.vstack([existing_data, combined])
    np.savetxt(os.path.join(
        final_dir, '{DATA_LAT_LON}'), output, delimiter='  ', fmt="%.5f")
