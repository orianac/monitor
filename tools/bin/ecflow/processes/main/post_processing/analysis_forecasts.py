''' storage_analysis.py
    usage: python storage_analysis.py config_file time_horizon_type
    time_horizon_type is MONITOR, MED_FCST, or SEAS_FCST and corresponds
    to section header in config file
    This script calculates percentiles for total column soil moisture,
    snow water equivalent (SWE) and total moisture (soil moisture + SWE)
    relative to the base period of 1981-2010.
'''
import os
from datetime import datetime, timedelta, date
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
        if (var == 'swe') and (xhist.mean() < 10):
            return np.nan
        elif (var == 'swe') and ((xhist.mean() > 10) and (current < 10)):
            return -1
        return stats.percentileofscore(
            historical[~np.isnan(historical)], current)
    return -9999


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
    elif percentile == -1:
        category = -1
    elif percentile == -9999:
        category = -9999
    else:
        category = np.nan
    return category

def add_metadata(ds):
    '''
    Add standard metadata to a dataset for publishing.
    '''

    ds.attrs['creator_name'] = 'Bart Nijssen'
    ds.attrs['creator_url'] = 'uwhydro.github.io'
    ds.attrs['creator_role'] = 'Co-investigator'
    ds.attrs['creator_email'] = 'nijssen@uw.edu'
    ds.attrs['institution'] = 'University of Washington'
    ds.attrs['processing_level'] = 'Gridded Hydrologic Projections'
    ds.attrs['acknowledgment'] = 'Please cite the references included herein. We acknowledge the NMME project(Kirtman et al 2014), which is responsible for NMME data. We thank the NMME project and the data dissemination supported by NOAA, NSF, NASA and DOEclimate modeling groups for producing and making available their model output. Additionally, we acknowledge the help of NCEP, IRI and NCAR personnel in creating, updating and maintaining the NMME archive'
    ds.attrs['references'] = '(NMME) Kirtman, B. P., Min, D., Infanti, J. M., Kinter III, J. L., Paolino, D. A., Zhang, Q., ... & Wood, E.F. (2013). The North American Multi-Model Ensemble (NMME): Phase-1 seasonal to interannual prediction, phase-2 toward developing intra-seasonal prediction. http://dx.doi.org/10.1175/BAMS-D-12-00050.1. (BCSD) Wood, A. W., and D. P. Lettenmaier, 2006: A test bed for new seasonal hydrologic forecasting approaches in the western United States. Bull. Amer. Meteor. Soc., 87, 1699–1712, doi:10.1175/BAMS-87-12-1699. (Skill of Downscaled NMME) Barbero, R., Abatzoglou, J.T. Hegewisch,K.C.. Evaluation of statistical downscaling of North American Multi-Model Ensemble forecasts over the Western United States. Weather and Forecasting, February 2017..DOI: 10.1175/WAF-D-16-0117.1. (VIC) Hamman, J. J., B. Nijssen, T. J. Bohn, D. R. Gergel, and Y. Mao, 2018: The Variable Infiltration Capacity Model, Version 5 (VIC-5): Infrastructure improvements for new applications and reproducibility. Geoscientific Model Development, doi:10.5194/gmd-11-3481-2018.'
    ds.attrs['standard_name_vocabulary'] = 'CF-1.0'
    ds.attrs['license'] = 'No restrictions'
    ds.attrs['contributor_name'] = 'Oriana Chegwidden'
    ds.attrs['contributor_role'] = 'Research Scientist'
    ds.attrs['contributor_email'] = 'orianc@uw.edu'
    ds.attrs['publishername'] = 'Northwest Knowledge Network'
    ds.attrs['publisheremail'] = 'info@northwestknowledge.net'
    ds.attrs['publisherurl'] = 'http://www.northwestknowledge.net'
    ds.attrs['date_modified'] = date.today().strftime("%Y-%m-%d")
    ds.attrs['date_issued'] = date.today().strftime("%Y-%m-%d")
    ds.attrs['geospatial_lat_units'] = 'decimal degrees north'
    ds.attrs['geospatial_lon_units'] = 'decimal degrees east'
    ds.attrs['geospatial_vertical_units'] = 'None'
    ds.attrs['geospatial_vertical_positive'] = 'Up'
    ds.attrs['project'] = 'Hydrologic outputs from downscaled NMME forecasts using BCSD and GRIDMET'
    return ds

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
    title = {'swe': 'SWE', 'sm': 'Total column soil moisture',
                     'tm': 'Total moisture storage',
             'ro': 'Total runoff'}
    if section == 'MED_FCST':
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        start_date = (datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') + timedelta(days=1))
        start_date_format = start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                start_date_format))
        print('open {}'.format(infile))
# this will open all of the different runs (we'll use this for the probability calculations down below)
        ensemble_xds = xr.open_mfdataset('/civil/hydro/climate_toolbox/run_vic/output/med_fcst/us/*/fluxes.{}.nc'.format(start_date_format), concat_dim='ensemble_member')
        forecast_xds = ensemble_xds.mean(dim='ensemble_member')
        forecast_xds.to_netcdf(os.path.join(config_dict[section]['OutputDirRoot'], 'mean_fluxes.{}.nc'.format(start_date_format)))
        #forecast_xds = xr.open_dataset(infile)
        # TODO: determine Oct1 flux file name in script instead of as input
        oct1_ds = xr.open_dataset(config_dict['MONITOR']['Oct1_flux_file'])
        print('open {}'.format(config_dict['PERCENTILES']['historic_VIC_out']))
        hist_ds = xr.open_dataset(config_dict['PERCENTILES']['historic_VIC_out'],
                               chunks={'lat': 100, 'lon': 100})
        time_frame_dict = {'wk1': {'start_day': 0, 'span': 7},
                           'wk2': {'start_day': 7, 'span': 7},
                           'wk3': {'start_day': 14, 'span': 7},
                           'wk4': {'start_day': 21, 'span': 7},
                           'wk12': {'start_day': 0, 'span': 14},
                           'wk123': {'start_day': 0, 'span': 21},
                           'wk1234': {'start_day': 0, 'span': 28},}
        for setup in time_frame_dict.keys(): # first do the calculations for the mean, then the probabilities
            start_day = time_frame_dict[setup]['start_day']
            span = time_frame_dict[setup]['span']
            scenarios = {'mean', 'ensemble'}
            # calculations for the mean
            curr_vals = {}
            print('get the variables we actually want')
                # remove Oct. 1 SWE at start of water year to correct for perennial SWE
            if start_date.month >= 10:
                oct_yr = start_date.year
            else:
                oct_yr = start_date.year - 1
            # fix the SWE values so that after the subtraction out of last year's Oct 1 snowpack
            # there is no negative value
            swe_subtracted = (forecast_xds['OUT_SWE'] -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-09-30'.format(
                            oct_yr))].values).values
            swe_subtracted[ swe_subtracted < 0 ] = 0
            # grab the last day of the period you're looking at
            curr_vals['swe'] = (xr.DataArray(swe_subtracted,
                                            coords=forecast_xds['OUT_SWE'].coords,
                                            dims=['time', 'lat', 'lon'])).isel(time=slice(start_day, start_day+span-1)).mean(dim='time')
            curr_vals['sm'] = forecast_xds['OUT_SOIL_MOIST'].sum(dim='nlayer',
                                         skipna=False).isel(time=slice(start_day, start_day+span-1)).mean(dim='time')
            curr_vals['tm'] = curr_vals['swe'] + curr_vals['sm']
            runoff = forecast_xds['OUT_RUNOFF']+forecast_xds['OUT_BASEFLOW']
            curr_vals['ro'] = runoff.isel(time=slice(start_day, start_day+span-1)).mean(dim='time')
            # this gets the analysis date to be in the middle of the period of interest
            analysis_date = start_date + timedelta(days=start_day+int(span/2))
            # find the integer value of the day of year for the center of the analysis period
            day = int(forecast_xds['OUT_SWE'].loc[dict(time=analysis_date)].time.dt.dayofyear)
            # Get historical CDF by selecting the window centered on the analysis day of year
            # with window width of the aggregation period (7, 15 or 30 days)
            hist_xds = hist_ds.where(
                (hist_ds['time'].dt.dayofyear >= day - int(span/2)) &
                (hist_ds['time'].dt.dayofyear <= day + int(span/2)))
            print('this is hist_xds for {}'.format(setup))
            print(hist_xds['sm'].mean().values)
            #for var in ['sm']:
            for var in ['swe', 'sm', 'tm', 'ro']:
                pname = '{0}percentile'.format(var)
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
                print(percentiles[pname].mean().values)
                percentiles['category'] = xr.apply_ufunc(return_category,
                                                 percentiles[pname],
                                                 dask='parallelized',
                                                 output_dtypes=[float],
                                                 vectorize=True)
                print('add attributes')
                percentiles[pname].attrs['_FillValue'] = np.nan
                percentiles['category'].attrs['_FillValue'] = np.nan
                percentiles.attrs['analysis_date'] = start_date_format
                percentiles = add_metadata(percentiles)
                percentiles.attrs['title'] = '{} percentiles'.format(title[var])
                #for attr in ['VIC_Model_Version', 'VIC_Driver',
                #     'references', 'Conventions', 'VIC_GIT_VERSION']:
                #    percentiles.attrs[attr] = forecast_xds.attrs[attr]
                print('save file')
                outfile = os.path.join(
                    config_dict[section]['Percentile_Loc'],
                    'vic-CFSv2_{0}_{1}_{2}.nc'.format(setup, pname, start_date_format))
                percentiles.to_netcdf(outfile)
                print('{} saved'.format(outfile))


    if section == 'SEAS_FCST':
        # load in the medium range forecast file as well
        model = 'CFSv2'
        med_met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
        print(os.path.join(med_met_fcst_loc, '%s.nc' % (model)))
    # Overwrite start_date to be one day after the final date of the monitor.
    # Overwrite end_date to be the final date of the medium range forecast.
        med_start_date = (datetime.strptime(config_dict['MONITOR']['End_Date'],
                  '%Y-%m-%d') + timedelta(days=1))
        med_start_date_format = med_start_date.strftime('%Y-%m-%d')
#        infile = os.path.join(
#            config_dict['MED_FCST']['OutputDirRoot'], 'fluxes.{}.nc'.format(
#                med_start_date_format))
        # load medium fcst
        med_fcst_xds = xr.open_dataset(os.path.join(config_dict['MED_FCST']['OutputDirRoot'], 'mean_fluxes.{}.nc'.format(med_start_date_format)))
        #med_fcst_xds = xr.open_mfdataset('/civil/hydro/climate_toolbox/run_vic/output/med_fcst/us/*/fluxes.2019-07-12.nc', concat_dim='ensemble_member')
        #med_fcst_xds = med_fcst_xds.mean(dim='ensemble_member')
        #med_forecast_xds = xr.open_dataset(infile)
        # load seasonal fcst
        model = 'NMME'
    # Overwrite start_date to be one day after the final date of the medium range forecast.
        # Overwrite end_date to be the final date of the seasonal range forecast.
        seas_start_date = pd.to_datetime(med_fcst_xds['time'].values[-1]) +\
                  timedelta(days=1)
        seas_start_date_format = seas_start_date.strftime('%Y-%m-%d')
        infile = os.path.join(
            config_dict[section]['OutputDirRoot'], 'fluxes.{}.nc'.format(
                seas_start_date_format))
        print('open {}'.format(infile))
        seas_forecast_xds = xr.open_dataset(infile)
        print(med_fcst_xds)
        print(seas_forecast_xds)
        seas_forecast_xds = seas_forecast_xds.mean(dim='nv')
        merged_forecast_xds = med_fcst_xds.combine_first(seas_forecast_xds).load()#, dim='time')
        # TODO: determine Oct1 flux file name in script instead of as input
        oct1_ds = xr.open_dataset(config_dict['MONITOR']['Oct1_flux_file'])
        # remove the start month since we only want month averages for the longer ranger forecast
        print(merged_forecast_xds['OUT_SWE'].time)
        date_stamps = merged_forecast_xds['OUT_SWE'].resample(freq='M', dim='time').time.values
        # get the months you'll need averaged over - they are the months after the current month
        # use the combined med range and seasonal forecast files - merge the files with xarray?
        # loop over those months
        for date_stamp in date_stamps:
            month_year_stamp = str(date_stamp)[0:7]
            print(month_year_stamp)
            month_num = int(month_year_stamp[5:7])
            print(month_num)
            curr_vals = {}
            print('get the variables we actually want')
                # remove Oct. 1 SWE at start of water year to correct for perennial SWE
            if med_start_date.month >= 10:
                oct_yr = med_start_date.year
            else:
                oct_yr = med_start_date.year - 1
            # fix the SWE values so that after the subtraction out of last year's Oct 1 snowpack
            # there is no negative value
            swe_subtracted = (merged_forecast_xds['OUT_SWE'] -
                        oct1_ds['OUT_SWE'].loc[dict(time='{}-09-30'.format(
                            oct_yr))].values).values
            swe_subtracted[ swe_subtracted < 0 ] = 0
            curr_vals['swe'] = (xr.DataArray(swe_subtracted,
                                            coords=merged_forecast_xds['OUT_SWE'].coords,
                                            dims=['time', 'lat', 'lon'])).groupby('time.month').first().sel(month=month_num)
            curr_vals['sm'] = merged_forecast_xds['OUT_SOIL_MOIST'].sum(dim='nlayer',
                                         skipna=False).groupby('time.month').first().sel(month=month_num)
            curr_vals['tm'] = curr_vals['swe'] + curr_vals['sm']
            runoff = merged_forecast_xds['OUT_RUNOFF']+merged_forecast_xds['OUT_BASEFLOW']
            curr_vals['ro'] = runoff.groupby('time.month').mean(dim='time').sel(month=month_num)
            # this gets the analysis date to be in the middle of the period of interest
            # Get historical CDF by selecting the window centered on the analysis day of year
            # with window width of the aggregation period (7, 15 or 30 days)
            print('open {}'.format(config_dict['PERCENTILES']['historic_VIC_out']))
            hist_xds = xr.open_dataset(config_dict['PERCENTILES']['historic_VIC_out'],
                               chunks={'lat': 100, 'lon': 100})
            hist_xds = hist_xds.where(hist_xds['time'].dt.month == month_num).where(hist_xds['time'].dt.day == 1)
            for var in ['swe', 'sm', 'tm', 'ro']:
                pname = '{}percentile'.format(var)
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
             #   percentiles[pname].attrs['missing_value'] = -1
            #    percentiles['category'].attrs['missing_value'] = -1
                percentiles['category'].attrs['_FillValue'] = np.nan
                percentiles.attrs['analysis_date'] = med_start_date_format
                percentiles.attrs['title'] = '{} percentiles: using downscaled BCSD-NMME run through the VIC hydrologic model'.format(title[var])
                percentiles.attrs['comment'] = ('Calculated from output from the'
                                        ' Variable Infiltration '
                                        'Capacity (VIC) Macroscale Hydrologic'
                                        ' Model')
                percentiles = add_metadata(percentiles)
               # for attr in ['VIC_Model_Version', 'VIC_Driver',
               #      'references', 'Conventions', 'VIC_GIT_VERSION']:
               #     percentiles.attrs[attr] = merged_forecast_xds.attrs[attr]
                print('save file')
                outfile = os.path.join(
                    config_dict[section]['Percentile_Loc'],
                    'vic-NMME_{0}_{1}_{2}.nc'.format(month_year_stamp,  pname,  med_start_date_format))
                percentiles.to_netcdf(outfile)
                print('{} saved'.format(outfile))


if __name__ == "__main__":
    main()
