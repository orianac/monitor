''' merge_forecasts.py
    usage: python merge_forecasts.py config_file time_horizon_type
    Uses cdo.mergetime to combine flux files into one file per year
    for forecasts. Because runoff percentiles are computed over time
    windows longer than forecasts, we merge forecasts with fluxes to create
    an annual forecast file '''
import os
import argparse
from datetime import datetime
from cdo import Cdo
import xarray as xr
from tonic.io import read_config
import pandas as pd
from datetime import datetime, timedelta

def main():
    ''' Uses cdo.mergetime to combine VIC flux files, keeping the data from the first
        input file when dates overlap '''
    # read in configuration file
    parser = argparse.ArgumentParser(description='Merge VIC flux files in ' +
                                     'time to have one flux file per year')
    parser.add_argument('config_file', metavar='config_file',
                        help='configuration file')
    parser.add_argument('time_horizon_type', help='MONITOR, MED_FCST, or ' +
                        'SEAS_FCST. Should correspond to section header in ' +
                        'config_file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)
    section = args.time_horizon_type
    cdo = Cdo()
    cdo.env['SKIP_SAME_TIME'] = '1'
    # get dates for file names by opening the metdata file
    ds = xr.open_dataset(config_dict[section]['Orig_Met'])
    if section == 'MED_FCST':
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        start_date = datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') + timedelta(days=1)
        start_date_format = start_date.strftime('%Y-%m-%d')
        end_date = pd.to_datetime(med_fcst_ds['time'].values[-1])
        end_date_format = end_date.strftime('%Y-%m-%d')
    elif section == 'SEAS_FCST':
    # Eventually, we might want to run multiple ensemble members. Dates could still be
    # read from the just one file but forcing = config_dict[section]['Orig_Met'] will
    # probably need to change
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the medium range forecast.
        # Overwrite end_date to be the final date of the seasonal range forecast.
        start_date = pd.to_datetime(med_fcst_ds['time'].values[-1]) +\
                  timedelta(days=1)
        start_date_format = start_date.strftime('%Y-%m-%d')
        model = 'ENSMEAN'
        seas_met_fcst_loc = config_dict['SEAS_FCST']['Met_Loc']
        seas_fcst_ds = xr.open_dataset(os.path.join(seas_met_fcst_loc, '%s.nc' % (model)))
        end_date = pd.to_datetime(seas_fcst_ds['time'].values[-1])
        end_date_format = end_date.strftime('%Y-%m-%d')
    # data is read from and saved to the same directory
    vic_dir = config_dict[section]['OutputDirRoot']
    print(start_date)
    print(end_date)
    print(type(start_date))
    print(type(end_date))
    print(type(start_date_format))
    print(type(end_date_format))
    vic_out = os.path.join(vic_dir, 'fluxes.{}.nc'.format(start_date))
    if section == 'MED_FCST':
        prev_vic_dir = config_dict['MONITOR']['OutputDirRoot']
    elif section == 'SEAS_FCST':
        prev_vic_dir = config_dict['MED_FCST']['OutputDirRoot']
    if end_date.year == start_date.year:
        # merge data into file from same year
        merged_out = os.path.join(vic_dir, 'fluxes.{}.nc'.format(
            start_date.year))
        print(merged_out)
        merged_in = os.path.join(prev_vic_dir, 'fluxes.{}.nc'.format(
            start_date.year))
        if os.path.isfile(merged_in) and os.path.isfile(vic_out):
            # keep values from previous sections
            cdo.mergetime(input='{0} {1}'.format(merged_in, vic_out),
                          output=merged_out)
    else:
        for year in range(start_date.year, end_date.year + 1):
            print(merged_out)
            # merge data into file from same year
            merged_out = os.path.join(vic_dir, 'fluxes.{}.nc'.format(year))
            merged_in = os.path.join(prev_vic_dir, 'fluxes.{}.nc'.format(year))
            if os.path.isfile(merged_in) and os.path.isfile(vic_out):
                tmp1 = cdo.seltime(year, input=vic_out)
                cdo.mergetime(input='{0} {1}'.format(merged_in, tmp1),
                              output=merged_out)


if __name__ == "__main__":
    main()
