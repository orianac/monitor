#!/usr/bin/env python
'''
get_med_metfcst.py
usage: <python> <get_med_metfcst.py> <configuration.cfg>
This script downloads downscaled CFSv2 90-day meteorological forecast
data from
https://tds-proxy.nkn.uidaho.edu/thredds/fileServer/
    NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/bcsd-nmme/cfsv2_metdata_90day/
delivered through OPeNDAP. Because attributes are lost during download,
they are added back in. To start, we just download multi-model ensemble mean.
'''
import os
import argparse
from datetime import datetime, timedelta
import numpy as np
import netCDF4
import xarray as xr
from cdo import Cdo
import cf_units

from tonic.io import read_config
from monitor import model_tools


def recreate_attrs(met_ds):
    ''' add attributes back into data sets because xarray will receive error
        "Illegal attribute" when opening url and delete all attributes'''
    # set up some geographic information
    esri_str = ("GEOGCS[\\\"GCS_WGS_1984\\\",DATUM" +
                "[\\\"D_WGS_1984\\\",SPHEROID\\\"WGS_1984\\\"," +
                "6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0]," +
                "UNIT[\\\"Degree\\\",0.0174532925199433]]")
    for var in met_ds.variables:
        met_ds[var].attrs['_FillValue'] = -32767.
        met_ds[var].attrs['missing_value'] = -32767.
        met_ds[var].attrs['esri_pe_string'] = esri_str
        met_ds[var].attrs['coordinates'] = 'lon lat'
    # global attributes
    met_ds.attrs['author'] = ('John Abatzoglou - University of ' +
                              'Idaho, jabatzoglou@uidaho.edu')
    met_ds.attrs['date'] = datetime.now().strftime('%d %B %Y')
    met_ds.attrs['note1'] = ('The projection information for this ' +
                             'file is: GCS WGS 1984.')
    met_ds.attrs['note2'] = ('These data were created using netCDF version 4.')
    met_ds.attrs['note3'] = ('Days correspond approximately to calendar ' +
                             'days ending at midnight, Mountain Standard ' +
                             'Time (7 UTC the next calendar day)')
    met_ds.attrs['note4'] = ('Bias corrected CFSV2 forecasts using the 4 ' +
                             'most recent CFSV2 runs each day, downscaled ' +
                             'to the gridMET (Abatzoglou, 2013) data.')
    # latitude attributes
    met_ds.lat.attrs['units'] = "degrees_north"
    met_ds.lat.attrs['description'] = "latitude"
    # longitude attributes
    met_ds.lon.attrs['units'] = "degrees_east"
    met_ds.lon.attrs['description'] = "longitude"
    # time attributes
    met_ds.time.attrs['units'] = "days since 1900-01-01 00:00:00"
    met_ds.time.attrs['calendar'] = "gregorian"
    met_ds.time.attrs['description'] = "days since 1900-01-01"
    # parameter attributes
    # precipitation
    met_ds.precipitation_amount.attrs['units'] = "mm"
    met_ds.precipitation_amount.attrs['description'] = ('Daily Accumulated' +
                                                        ' Precipitation')
    met_ds.precipitation_amount.attrs['cell_methods'] = ('time: sum(' +
                                                         'intervals: 24 hours)')
    # temperature
    met_ds.tmmn.attrs['description'] = "Daily Minimum Temperature"
    met_ds.tmmx.attrs['cell_methods'] = "time: minimum(interval: 24 hours)"
    met_ds.tmmx.attrs['description'] = "Daily Maximum Temperature"
    met_ds.tmmx.attrs['cell_methods'] = "time: maximum(interval: 24 hours)"
    for var in ['tmmn', 'tmmx']:
        met_ds[var].attrs['units'] = "degC"
        met_ds[var].attrs['height'] = "2 m"
    # wind speed
    met_ds.wind_speed.attrs['units'] = "m/s"
    met_ds.wind_speed.attrs['description'] = "Daily Mean Wind Speed"
    met_ds.wind_speed.attrs['height'] = "10 m"
    # shortwave radiation
    met_ds.surface_downwelling_shortwave_flux_in_air.attrs['units'] = "W m-2"
    met_ds.surface_downwelling_shortwave_flux_in_air.attrs['description'] = (
        'Daily Mean Downward Shortwave Radiation At Surface')
    # specific humidity
    met_ds.specific_humidity.attrs['units'] = "kg/kg"
    met_ds.specific_humidity.attrs['description'] = "Daily Mean Specific " + \
        "Humidity"
    met_ds.specific_humidity.attrs['height'] = "2 m"
    return met_ds


def main():
    ''' Download meteorological forecast data for 90-day forecast
        from http://thredds.northwestknowledge.net:8080/thredds/catalog/
        NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/cfsv2_metdata_90day/catalog.html
    '''
    # read in configuration file
    parser = argparse.ArgumentParser(description='Download met forecast data')
    parser.add_argument('config_file', metavar='config_file',
                        help='configuration file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)

    # initialize cdo
    cdo = Cdo()

    # read in meteorological data location
    met_fcst_loc = config_dict['MED_FCST']['Met_Loc']
    old_config_file = config_dict['ECFLOW']['old_Config']
    new_config_file = config_dict['ECFLOW']['new_Config']

    # read in grid_file from config file
    grid_file = config_dict['DOMAIN']['GridFile']

    # define variable names used when filling threads URL
    # an abbreviation and a full name is needed
    varnames = {'vs': 'wind_speed', 'tmmx': 'air_temperature',
                'tmmn': 'air_temperature',
                'srad': 'surface_downwelling_shortwave_flux_in_air',
                'sph': 'specific_humidity', 'pr': 'precipitation_amount',
                'pet': 'potential_evapotranspiration'}
    # define model names for file name
    modelnames = ['CFSv2']
    # ['NCAR', 'NASA', 'GFDL', 'GFDL-FLOR', 'ENSMEAN', 'CMC1', 'CMC2',
    #              'CFSv2']
    new_units = {'vs': 'm s-1', 'sph': 'kg kg-1',
                 'tmmx': 'degC', 'tmmn': 'degC', 'srad': 'W m-2',
                 'pr': 'mm', 'pet': 'mm'}
    old_units = {'vs': 'm s-1', 'sph': 'kg kg-1',
                 'tmmx': 'K', 'tmmn': 'K', 'srad': 'W m-2',
                 'pr': 'mm', 'pet': 'mm'}

    # download metdata from http://thredds.northwestknowledge.net
    for model in modelnames:
        dlist = []
        for var, name in varnames.items():
            url = ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/' +
                   'NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/' +
                   'cfsv2_metdata_90day/' +
                   'cfsv2_metdata_forecast_%s_daily.nc' % (var))
            print('Reading {0}'.format(url))
            xds = xr.open_dataset(url)
            if name == 'air_temperature':
                # Change variable names so that tmmn and tmax are different
                xds.rename({'air_temperature': var}, inplace=True)
            dlist.append(xds)
        merge_ds = xr.merge(dlist)
        # MetSim requires time dimension be named "time"
        merge_ds.rename({'day': 'time'}, inplace=True)
        for var in ('tmmn', 'tmmx'):
            units_in = cf_units.Unit(old_units[var])
            units_out = cf_units.Unit(new_units[var])
            # Perform units conversion
            units_in.convert(merge_ds[var].values[:], units_out, inplace=True)
            # Fix _FillValue after unit conversion
            merge_ds[var].values[merge_ds[var].values < -30000] = -32767.
        # MetSim requires time dimension be named "time"
        merge_ds = merge_ds.transpose('time', 'lat', 'lon')
        merge_ds = recreate_attrs(merge_ds)
        # Make sure tmax >= tmin always
        tmin = np.copy(merge_ds['tmmn'].values)
        tmax = np.copy(merge_ds['tmmx'].values)
        swap_values = ((tmin > tmax) & (tmax != -32767.))
        merge_ds['tmmn'].values[swap_values] = tmax[swap_values]
        merge_ds['tmmx'].values[swap_values] = tmin[swap_values]
        time = merge_ds['time']
        end_date = netCDF4.num2date(time[-1], time.units, time.calendar)
        start_date = datetime.strptime(config_dict['MONITOR']['End_Date'],
                                       '%Y-%m-%d') + timedelta(days=1)
        start_date_format = start_date.strftime('%Y-%m-%d')
        end_date_format = end_date.strftime('%Y-%m-%d')
        vic_save_state_format = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')
        # replace start date, end date and met location in the configuration
        # file. SEAS_* variables will be defined in get_seasonal_metfcst.py.
        # replace those keywords with dummy variable (-9999) so that
        # replace_var_pythonic_config() won't fail.
        kwargs = {'START_DATE': config_dict['MONITOR']['Start_Date'],
                  'END_DATE': config_dict['MONITOR']['End_Date'],
                  'VIC_SAVE_STATE': config_dict['MONITOR']['vic_save_state'],
                  'MED_START_DATE': start_date_format,
                  'MED_END_DATE': end_date_format,
                  'MED_VIC_SAVE_STATE': vic_save_state_format,
                  'SEAS_START_DATE': -9999,
                  'SEAS_END_DATE': -9999,
                  'SEAS_VIC_SAVE_STATE':-9999}
        model_tools.replace_var_pythonic_config(old_config_file,
                                                new_config_file,
                                                header=None, **kwargs)

        outfile = os.path.join(met_fcst_loc, '%s.nc' % (model))
        print('Conservatively remap and write to {0}'.format(outfile))
        cdo.remapcon(grid_file, input=merge_ds, output=outfile)


if __name__ == "__main__":
    main()
