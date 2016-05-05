#!/usr/bin/env python
#####tocel_combine.py

#####combine.py runs this script
#####by replacing the ALL CAPITAL words
#####and using subprocess.
#####This script changes tmin and tmax units from Kelvin to celcius
#####and combines precip, tmax, tmin, and wind into 1 file for each grid cell. 

import pandas as pd
import numpy as np

tmin_dir = 'TMIN_DIREC'
tmax_dir = 'TMAX_DIREC'
precip_dir = 'PRECIP_DIREC'
wind_dir = 'WIND_DIREC'
final_dir = 'FINAL_DIREC'

#temperature data is in kelvin, switch to celcius

tmin_kel = np.genfromtxt('%sDATA_LAT_LON' %(tmin_dir))
tmax_kel = np.genfromtxt('%sDATA_LAT_LON' %(tmax_dir))

tmin_cel = tmin_kel - 273.15
tmax_cel = tmax_kel - 273.15

#columns are based on vic forcings input

precip = np.genfromtxt('%sDATA_LAT_LON' %(precip_dir))
wind = np.genfromtxt('%sDATA_LAT_LON' %(wind_dir))

precip = float(precip)
wind = float(wind)

d = {'precipitation': format(precip, '.5f'), 'tmax': format(tmax_cel, '.5f'), 'tmin': format(tmin_cel, '.5f'), 'wind': format(wind, '.5f')}
df = pd.DataFrame(data=d, index=['parameters'])

df.to_csv('%sDATA_LAT_LON' %(final_dir), sep='\t', header=False, index=False)
