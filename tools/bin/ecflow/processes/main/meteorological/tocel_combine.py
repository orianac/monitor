#!/usr/bin/env python
"""
tocel_combine.py

combine.py runs this script
by replacing the ALL CAPITAL words
and using subprocess.
This script changes tmin and tmax units from Kelvin to celsius
and combines precip, tmax, tmin, and wind into 1 file for each grid cell. 
"""
import pandas as pd
import numpy as np
from monitor.share import KELVIN

#these paths will be filled in by combine.py
tmin_dir = '{TMIN_DIREC}'
tmax_dir = '{TMAX_DIREC}'
precip_dir = '{PRECIP_DIREC}'
wind_dir = '{WIND_DIREC}'
final_dir = '{FINAL_DIREC}'

#read in tmin and tmax
#temperature data is in kelvin, switch to celcius
tmin_kel = np.genfromtxt('%s{DATA_LAT_LON}' %(tmin_dir))
tmax_kel = np.genfromtxt('%s{DATA_LAT_LON}' %(tmax_dir))

tmin_cel = tmin_kel - KELVIN
tmax_cel = tmax_kel - KELVIN

#read in precip and wind
precip = np.genfromtxt('%s{DATA_LAT_LON}' %(precip_dir), dtype='float')
wind = np.genfromtxt('%s{DATA_LAT_LON}' %(wind_dir), dtype='float')

#create dictionary in the order that VIC reads in forcings parameters
d = {'precipitation': format(precip, '.5f'), 'tmax': format(tmax_cel, '.5f'), 'tmin': format(tmin_cel, '.5f'), 'wind': format(wind, '.5f')}

df = pd.DataFrame(data=[("%.5f" % precip, "%.5f" % tmax_cel,
        "%.5f" % tmin_cel, "%.5f" % wind)], 
	columns=['precipitation', 'tmax', 'tmin', 'wind'], 
	index=['parameters'])
#save
df.to_csv('%s{DATA_LAT_LON}' %(final_dir), sep='\t', header=False, index=False)
