#!/usr/bin/env python
"""
tocel_combine.py

combine.py runs this script
by replacing the ALL CAPITAL words
and using subprocess.
This script changes tmin and tmax units from Kelvin to Celsius
and combines precip, tmax, tmin, and wind into 1 file for each grid cell. 
"""
import pandas as pd
import numpy as np
from monitor.share import KELVIN

# these paths will be filled in by combine.py
tmin_dir = '{TMIN_DIREC}'
tmax_dir = '{TMAX_DIREC}'
precip_dir = '{PRECIP_DIREC}'
wind_dir = '{WIND_DIREC}'
final_dir = '{FINAL_DIREC}'

#read in tmin and tmax
# temperature data is in Kelvin, switch to Celcius
tmin_kel = np.genfromtxt(os.path.join(tmin_dir, '{DATA_LAT_LON}'))
tmax_kel = np.genfromtxt(os.path.join(tmax_dir, '{DATA_LAT_LON}'))

tmin_cel = tmin_kel - KELVIN
tmax_cel = tmax_kel - KELVIN

#read in precip and wind
precip = np.genfromtxt(os.path.join(
    precip_dir, '{DATA_LAT_LON}'), dtype='float')
wind = np.genfromtxt(os.path.join(wind_dir, '{DATA_LAT_LON}'), dtype='float')

# create a dataframe in the order that VIC reads in forcings parameters
df = pd.DataFrame(data=[("%.5f" % precip, "%.5f" % tmax_cel,
                         "%.5f" % tmin_cel, "%.5f" % wind)],
                  columns=['precipitation', 'tmax', 'tmin', 'wind'],
                  index=['parameters'])
# save
df.to_csv(os.path.join(final_dir, '{DATA_LAT_LON}'),
          sep='\t', header=False, index=False)
