#!/usr/bin/env python2.7

import xray
import os
import time
import datetime
import numpy as np
import pandas as pd
import sys
from tonic.io import read_netcdf
from datetime import date



this_year = date.today().year
last_year = date.today().year - 1



direc = sys.argv[1]

#direc = '/raid3/mbapt/ecflow/pnw/met_files'

try:
        file = 'pr_%s.nc' %(this_year)
        ds = xray.open_dataset(os.path.join(direc, file))                    
except RuntimeError:
        file = 'pr_%s.nc' %(last_year)
        ds = xray.open_dataset(os.path.join(direc, file))

time_var = 'day'
date_direc = 'textfiles' 
date_file = 'date.txt'

dates = ds[time_var].values
recent_date = max(dates)
ts = pd.to_datetime(str(recent_date))
date2 = ts.strftime('%Y-%m-%d')

yesterday = np.genfromtxt('%s/%s' %(date_direc, date_file), skip_header=0, dtype='str', delimiter=' ', usecols=0)
yesterday2 = np.array_str(yesterday)
print(date2)
print(yesterday2)
if date2 > yesterday2:
       	n_date_file = open('%s/%s' %(date_direc, date_file), "w")
       	n_date_file.write(date2)
       	n_date_file.close()
else:
       	raise ValueError('Not New Data')
