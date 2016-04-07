#!/usr/bin/env python

import sys
from datetime import datetime, timedelta
from core.io import proc_subprocess
from core.io import replace

config_file = sys.argv[1]
met_loc = sys.argv[2]


#how many days behind metdata is from realtime 
N = 1
#number of days VIC will run for
period = 7

date_end = datetime.now() - timedelta(days=N)
num_day_end = date_end.timetuple().tm_yday - 1
year_end = date_end.strftime('%Y')
date_end1= date_end.strftime('%Y-%m-%d')

date_begin = datetime.now() - timedelta(days=period+N-1)
num_day_begin = date_begin.timetuple().tm_yday - 1
year_begin = date_begin.strftime('%Y')
date_begin1= date_begin.strftime('%Y-%m-%d')

#download metdata from http://thredds.northwestknowledge.net
param = ['pr', 'tmmn', 'tmmx', 'vs']

#if days fall in the same year
if num_day_begin < num_day_end:
    
    for i in param:
        get_metdata = ['wget', '-nc -c -nd --no-check-certificate', 
                       'http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/%s/%s_%s.nc?day[%s:1:%s]' %(i, i, year_end, num_day_begin, num_day_end)]
        proc_subprocess(get_metdata, met_loc)
#if days fall in different years        
else: 
    num_days = period - num_day_end
        
        if num_days > 1:
            num_day_mid = num_day_begin + num_days - 1
            for i in param:
                get_metdata = ['wget', '-nc -c -nd --no-check-certificate', 
                       'http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/%s/%s_%s.nc?day[0:1:%s]' %(i, i, year_end, num_day_end)]
                proc_subprocess(get_metdata, met_loc)
                get_metdata2 = ['wget', '-nc -c -nd --no-check-certificate', 
                       'http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/%s/%s_%s.nc?day[%s:1:%s]' %(i, i, year_begin, num_day_begin, num_day_mid)]
                #combine both years of data into one file named after new year
                cdo_cat = ['cdo', 'cat', '%s_%s.nc' %(i, year_end), '%s_%s.nc' %(i, year_begin), '%s_%s.nc' %(i, year_end)]
                proc_subprocess(cdo_cat, met_loc)
        else:
            for i in param:
                get_metdata = ['wget', '-nc -c -nd --no-check-certificate', 
                       'http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/%s/%s_%s.nc?day[0:1:%s]' %(i, i, year_end, num_day_end)]
                proc_subprocess(get_metdata, met_loc)
                get_metdata2 = ['wget', '-nc -c -nd --no-check-certificate', 
                       'http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/%s/%s_%s.nc?day[%s]' %(i, i, year_begin, num_day_begin)]
                #combine both years of data into one file named after new year
                cdo_cat = ['cdo', 'cat', '%s_%s.nc' %(i, year_end), '%s_%s.nc' %(i, year_begin), '%s_%s.nc' %(i, year_end)]
                proc_subprocess(cdo_cat, met_loc)

#replace start date, end date and year in the configuation file
replace(config_file, 'MODEL_START_DATE', date_begin1)
replace(config_file, 'MODEL_END_DATE', date_end1)
replace(config_file, 'METDATA_YEAR', year_end)
