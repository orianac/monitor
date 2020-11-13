#!/bin/python

import schedule
import time
import subprocess
from datetime import datetime, timedelta
import os
import numpy as np
import functools

def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                print(traceback.format_exc())
                if cancel_on_failure:
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator

@catch_exceptions(cancel_on_failure=False)
def run_toolbox():
    # check to see how many days are missing to backfill those
    daysback = 0
    date_ran_successfully = False
# Check the last time it ran successfully
    while not date_ran_successfully:
        daysback+=1
        first_date_to_run = datetime.now() - timedelta(days=daysback)
        date_to_search_for_in_files = first_date_to_run.strftime('%Y-%m-%d')
        forecast_file_to_check = '/pool0/data/orianac/climate_toolbox/post_processing/cdf_results/us/vic-metdata_ropercentile_ccy_{date}.nc'
        forecast_files_to_find = forecast_file_to_check.format(date=date_to_search_for_in_files)
        date_ran_successfully = os.path.exists(forecast_files_to_find)
    days_to_run = np.arange(daysback)[::-1][:-1]
    print(days_to_run)
    if len(days_to_run) > 0:
        print('running at {}'.format(date_ran_successfully))
        subprocess.check_call(['./run_monitor.sh {}'.format(" ".join(str(num) for num in days_to_run))], shell=True)
    subprocess.check_call(['./run_forecasts.sh'], shell=True)

@catch_exceptions(cancel_on_failure=False)
def run_forecasts():
    subprocess.check_call(['./run_forecasts.sh'], shell=True)

schedule.every().day.at('16:50').do(run_toolbox)

while True:
    schedule.run_pending()
    time.sleep(1)
