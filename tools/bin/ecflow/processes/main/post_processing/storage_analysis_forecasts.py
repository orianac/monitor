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
    # assign the analysis date differently depending on when you're calculating for the percentile
    if section == 'MED_FCST':
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        start_date = (datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') - timedelta(days=1))
        start_date_format = start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                start_date_format))
#        print(analysis_date)
    elif section == 'SEAS_FCST':
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        med_fcst_ds = xr.open_dataset(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the medium range forecast.
        # Overwrite end_date to be the final date of the seasonal range forecast.
        start_date = pd.to_datetime(med_fcst_ds['time'].values[-1]) +\
                  timedelta(days=1)
        start_date_format = start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                start_date_format))
#        analysis_date = start_date + timedelta(days=110)
    print('open {}'.format(infile))
    forecast_xds = xr.open_dataset(infile)
    # TODO: determine Oct1 flux file name in script instead of as input
    oct1_ds = xr.open_dataset(config_dict['MONITOR']['Oct1_flux_file'])
    print('open {}'.format(config_dict['PERCENTILES']['historic_VIC_out']))
    hist_xds = xr.open_dataset(config_dict['PERCENTILES']['historic_VIC_out'],
                               chunks={'lat': 100, 'lon': 100})
    if section == 'MED_FCST':
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
                # extract variables
            swe_subtracted = (forecast_xds['OUT_SWE'].rolling(time=agg,
                        center=True).mean().isel(time=agg) -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-10-01'.format(
                            oct_yr))].values).values
            template_array = (forecast_xds['OUT_SWE'].rolling(time=agg,
                        center=True).mean().isel(time=agg) -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-10-01'.format(
                            oct_yr))].values)
            # fix the SWE values so that after the subtraction out of last year's Oct 1 snowpack
            # there is no negative value
            swe_subtracted[ swe_subtracted < 0 ] = 0
            print(np.nanmean(hist_xds['swe'].values))
            curr_vals['swe'] = xr.DataArray(swe_subtracted,
                                            coords=forecast_xds['OUT_SWE'].rolling(
                                                          time=agg).mean().isel(time=agg).coords,
                                            dims=['lat', 'lon'])
            curr_vals['sm'] = forecast_xds['OUT_SOIL_MOIST'].sum(dim='nlayer',
                                         skipna=False).rolling(
                                        time=agg, center=True).mean().isel(time=agg)
            curr_vals['tm'] = curr_vals['swe'] + curr_vals['sm']
            day = int(forecast_xds['OUT_SWE'].loc[dict(time=start_date)].time.dt.dayofyear)
            print(day)
            # Get historical CDF by selecting the five days centerd on the analysis day of year
            hist_xds = hist_xds.where(
                (hist_xds['time'].dt.dayofyear >= day - 2) &
                (hist_xds['time'].dt.dayofyear <= day + 2))
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


if __name__ == "__main__":
    main()
