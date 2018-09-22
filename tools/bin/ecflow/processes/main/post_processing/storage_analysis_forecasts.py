''' storage_analysis.py
    usage: python storage_analysis.py config_file time_horizon_type
    time_horizon_type is MONITOR, MED_FCST, or SEAS_FCST and corresponds
    to section header in config file
    This script calculates percentiles for total column soil moisture,
    snow water equivalent (SWE) and total moisture (soil moisture + SWE)
    relative to the base period of 1981-2010.
'''
import os
from datetime import datetime, timedelta
import argparse
import numpy as np
from scipy import stats
import xarray as xr
import pandas as pd
from tonic.io import read_config
import calendar

def run_percentileofscore(historical, current, var):
    ''' Remove bad data and run stats.percentileofscore(). For SWE,
        require that the historical mean and current value are both
        at least 10 mm '''
    xhist = historical[~np.isnan(historical)]
    xhist = xhist[xhist != np.nan]
    if xhist.size:
        # apply 10mm threshold to SWE returning nan if below threshold
        if (var == 'swe') and ((xhist.mean() < 10) or (current < 10)):
            return np.nan
        return stats.percentileofscore(
            historical[~np.isnan(historical)], current)
    return np.nan


def return_category(percentile):
    ''' Assign percentiles to categories to create a non-linear
        color bar corresponding to to the U.S. Drought Monitor '''
    if 0 <= percentile < 2:
        category = 0
    elif 2 <= percentile < 5:
        category = 1
    elif 5 <= percentile < 10:
        category = 2
    elif 10 <= percentile < 20:
        category = 3
    elif 20 <= percentile < 30:
        category = 4
    elif 30 <= percentile < 70:
        category = 5
    elif 70 <= percentile < 80:
        category = 6
    elif 80 <= percentile < 90:
        category = 7
    elif 90 <= percentile < 95:
        category = 8
    elif 95 <= percentile < 98:
        category = 9
    elif percentile >= 98:
        category = 10
    else:
        category = -9999
    return category


def main():
    ''' Calculate percentiles for SWE, total moisture (SWE + soil moisture)
        and soil moisture '''
    parser = argparse.ArgumentParser(
        description='Calculate storage percentiles')
    parser.add_argument('config_file', metavar='config_file',
                        help='the python configuration file, see template:'
                        ' /monitor/config/python_template.cfg')
    parser.add_argument('time_horizon_type', help='MONITOR, MED_FCST, or '
                        'SEAS_FCST. Should correspond to section header in '
                        'config_file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)
    section = args.time_horizon_type
    if section == 'MED_FCST':
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        start_date = (datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') + timedelta(days=1))
        start_date_format = start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                start_date_format))
        print('open {}'.format(infile))
        forecast_xds = xr.open_dataset(infile)
        # TODO: determine Oct1 flux file name in script instead of as input
        oct1_ds = xr.open_dataset(config_dict['MONITOR']['Oct1_flux_file'])
        print('open {}'.format(config_dict['PERCENTILES']['historic_VIC_out']))
        hist_xds = xr.open_dataset(config_dict['PERCENTILES']['historic_VIC_out'],
                               chunks={'lat': 100, 'lon': 100})
        for agg in [7, 15, 30]:
            curr_vals = {}
          #  for var in ['swe', 'sm', 'tm']:
                    # extract variables
            print('get the variables we actually want')
#            agg_xds = forecast_xds['OUT_SWE'].rolling(time=agg).mean()
#            today = agg_xds.loc[dict(time=start_date)]
            title = {'swe': 'SWE', 'sm': 'Total column soil moisture',
                     'tm': 'Total moisture storage'}
                # remove Oct. 1 SWE at start of water year to correct for perennial SWE
            if start_date.month >= 10:
                oct_yr = start_date.year
            else:
                oct_yr = start_date.year - 1
            # fix the SWE values so that after the subtraction out of last year's Oct 1 snowpack
            # there is no negative value
            swe_subtracted = (forecast_xds['OUT_SWE'] -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-10-01'.format(
                            oct_yr))].values).values
            swe_subtracted[ swe_subtracted < 0 ] = 0
            curr_vals['swe'] = (xr.DataArray(swe_subtracted,
                                            coords=forecast_xds['OUT_SWE'].coords,
                                            dims=['time', 'lat', 'lon'])).isel(time=slice(0,agg)).mean(dim='time')
            curr_vals['swe'].to_netcdf('/civil/hydro/climate_toolbox/post_processing/cdf_results/us/junk.nc')
            curr_vals['sm'] = forecast_xds['OUT_SOIL_MOIST'].sum(dim='nlayer',
                                         skipna=False).isel(time=slice(0,agg)).mean(dim='time')
            curr_vals['tm'] = curr_vals['swe'] + curr_vals['sm']
            # this gets the analysis date to be in the middle of the period of interest
            analysis_date = start_date + timedelta(days=int(agg/2))
            day = int(forecast_xds['OUT_SWE'].loc[dict(time=start_date)].time.dt.dayofyear)
            # Get historical CDF by selecting the window centered on the analysis day of year
            # with window width of the aggregation period (7, 15 or 30 days)
            hist_xds = hist_xds.where(
                (hist_xds['time'].dt.dayofyear >= day - int(agg/2)) &
                (hist_xds['time'].dt.dayofyear <= day + int(agg/2)))
            print(hist_xds.dims)
            print(hist_xds.coords)
            print('ABOVE!!!!!!!!')
            for var in ['swe', 'sm', 'tm']:
                pname = '{0}day_{1}percentile'.format(agg, var)
                percentiles = xr.Dataset({pname:
                                  (['lat', 'lon'],
                                   np.nan * np.ones(curr_vals[var].shape)),
                                  'category':
                                  (['lat', 'lon'],
                                   np.nan * np.ones(curr_vals[var].shape))},
                                 coords={'lon': curr_vals[var].lon,
                                         'lat': curr_vals[var].lat})
                print('calculate percentiles for {}'.format(var))
                percentiles[pname] = xr.apply_ufunc(
                    run_percentileofscore, hist_xds[var], curr_vals[var],
                    kwargs={'var': var}, input_core_dims=[['time'], []],
                    dask='parallelized', output_dtypes=[float], vectorize=True)
                print('calculate category for {}'.format(var))
                percentiles['category'] = xr.apply_ufunc(return_category,
                                                 percentiles[pname],
                                                 dask='parallelized',
                                                 output_dtypes=[float],
                                                 vectorize=True)
                print('add attributes')
                percentiles[pname].attrs['_FillValue'] = np.nan
                percentiles['category'].attrs['_FillValue'] = np.nan
                percentiles.attrs['analysis_date'] = start_date_format
                percentiles.attrs['title'] = '{} percentiles'.format(title[var])
                percentiles.attrs['comment'] = ('Calculated from output from the'
                                        ' Variable Infiltration '
                                        'Capacity (VIC) Macroscale Hydrologic'
                                        ' Model')
                for attr in ['VIC_Model_Version', 'VIC_Driver',
                     'references', 'Conventions', 'VIC_GIT_VERSION']:
                    percentiles.attrs[attr] = forecast_xds.attrs[attr]
                print('save file')
                outfile = os.path.join(
                    config_dict[section]['Percentile_Loc'],
                    'vic-forecast_{0}_{1}.nc'.format(pname, start_date_format))
                percentiles.to_netcdf(outfile)
                print('{} saved'.format(outfile))


    if section == 'SEAS_FCST':
        # load in the medium range forecast file as well
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        print(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        med_start_date = (datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') + timedelta(days=1))
        med_start_date_format = med_start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict['MED_FCST']['OutputDirRoot'], 'fluxes.{}.nc'.format(
                med_start_date_format))
        # load medium fcst
        med_forecast_xds = xr.open_dataset(infile)
        # load seasonal fcst
        model = 'CFSv2'
    # Overwrite start_date to be one day after the final date of the medium range forecast.
        # Overwrite end_date to be the final date of the seasonal range forecast.
        seas_start_date = pd.to_datetime(med_fcst_ds['time'].values[-1]) +\
                  timedelta(days=1)
        seas_start_date_format = seas_start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                seas_start_date_format))
        print('open {}'.format(infile))
        seas_forecast_xds = xr.open_dataset(infile)
        merged_forecast_xds = xr.concat([med_forecast_xds, seas_forecast_xds], dim='time')
        # TODO: determine Oct1 flux file name in script instead of as input
        oct1_ds = xr.open_dataset(config_dict['MONITOR']['Oct1_flux_file'])
        month_nums = merged_forecast_xds['OUT_SWE'].groupby('time.month').mean().month.values
        # remove the start month since we only want month averages for the longer ranger forecast
        month_nums = month_nums[month_nums!=med_start_date.month]
        month_names = [calendar.month_abbr[month_integer] for month_integer in month_nums]
        # get the months you'll need averaged over - they are the months after the current month
        # use the combined med range and seasonal forecast files - merge the files with xarray?
        # loop over those months
        for (month_num, month_name) in zip(month_nums, month_names):
            curr_vals = {}
            print('get the variables we actually want')
            title = {'swe': 'SWE', 'sm': 'Total column soil moisture',
                     'tm': 'Total moisture storage'}
                # remove Oct. 1 SWE at start of water year to correct for perennial SWE
            if med_start_date.month >= 10:
                oct_yr = med_start_date.year
            else:
                oct_yr = med_start_date.year - 1
            # fix the SWE values so that after the subtraction out of last year's Oct 1 snowpack
            # there is no negative value
            swe_subtracted = (merged_forecast_xds['OUT_SWE'] -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-10-01'.format(
                            oct_yr))].values).values
            swe_subtracted[ swe_subtracted < 0 ] = 0
            curr_vals['swe'] = (xr.DataArray(swe_subtracted,
                                            coords=merged_forecast_xds['OUT_SWE'].coords,
                                            dims=['time', 'lat', 'lon'])).groupby('time.month').mean(
                                             dim='time').sel(month=month_num)
            curr_vals['sm'] = merged_forecast_xds['OUT_SOIL_MOIST'].sum(dim='nlayer',
                                         skipna=False).groupby('time.month').mean(
                                             dim='time').sel(month=month_num)
            curr_vals['tm'] = curr_vals['swe'] + curr_vals['sm']
            # this gets the analysis date to be in the middle of the period of interest
            # Get historical CDF by selecting the window centered on the analysis day of year
            # with window width of the aggregation period (7, 15 or 30 days)
            print('open {}'.format(config_dict['PERCENTILES']['historic_VIC_out']))
            hist_xds = xr.open_dataset(config_dict['PERCENTILES']['historic_VIC_out'],
                               chunks={'lat': 100, 'lon': 100})
            hist_xds = hist_xds.where(hist_xds['time'].dt.month == month_num)
            for var in ['swe', 'sm', 'tm']:
                pname = '{0}_average_{1}percentile'.format(month_name, var)
                percentiles = xr.Dataset({pname:
                                  (['lat', 'lon'],
                                   np.nan * np.ones(curr_vals[var].shape)),
                                  'category':
                                  (['lat', 'lon'],
                                   np.nan * np.ones(curr_vals[var].shape))},
                                 coords={'lon': curr_vals[var].lon,
                                         'lat': curr_vals[var].lat})
                print('calculate percentiles for {}'.format(var))
                percentiles[pname] = xr.apply_ufunc(
                    run_percentileofscore, hist_xds[var], curr_vals[var],
                    kwargs={'var': var}, input_core_dims=[['time'], []],
                    dask='parallelized', output_dtypes=[float], vectorize=True)
                print('calculate category for {}'.format(var))
                percentiles['category'] = xr.apply_ufunc(return_category,
                                                 percentiles[pname],
                                                 dask='parallelized',
                                                 output_dtypes=[float],
                                                 vectorize=True)
                print('add attributes')
                percentiles[pname].attrs['_FillValue'] = np.nan
                percentiles['category'].attrs['_FillValue'] = np.nan
                percentiles.attrs['analysis_date'] = med_start_date_format
                percentiles.attrs['title'] = '{} percentiles'.format(title[var])
                percentiles.attrs['comment'] = ('Calculated from output from the'
                                        ' Variable Infiltration '
                                        'Capacity (VIC) Macroscale Hydrologic'
                                        ' Model')
                for attr in ['VIC_Model_Version', 'VIC_Driver',
                     'references', 'Conventions', 'VIC_GIT_VERSION']:
                    percentiles.attrs[attr] = merged_forecast_xds.attrs[attr]
                print('save file')
                outfile = os.path.join(
                    config_dict[section]['Percentile_Loc'],
                    'vic-forecast_{0}_{1}.nc'.format(pname, med_start_date_format))
                percentiles.to_netcdf(outfile)
                print('{} saved'.format(outfile))


if __name__ == "__main__":
    main()
