#!/usr/bin/env python
"""
transferring percentile files
usage: <python> <file_transfer.py> <configuration.cfg> <Section>

Uses paramiko to transfer percentile netCDFs to the
NKN network, along with a text file that includes the date.
The date is read from that file and displayed in the map subtitles.
"""

import os
import argparse
from datetime import datetime, timedelta
import paramiko
import pandas as pd
from tonic.io import read_config


def main():
    ''' Transfer percentile files to NKN network '''
    # read in configuration file
    parser = argparse.ArgumentParser(description='Upload percentiles')
    parser.add_argument('config_file', metavar='config_file',
                        help='configuration file')
    parser.add_argument('time_horizon_type', help='MONITOR, MED_FCST, or '
                        'SEAS_FCST. Should correspond to section header in '
                        'config_file')
    args = parser.parse_args()
    config_dict = read_config(args.config_file)
    section = args.time_horizon_type
    # read in the source and destination paths and current date
    source_loc = config_dict[section]['Percentile_Loc']
    dest_loc = config_dict[section]['Percentile_Dest']
    date = config_dict['MONITOR']['End_Date']
    print(date)
    current_date = datetime.strptime(date, '%Y-%m-%d')
    month = current_date.strftime("%B_%-d").upper()[:3]

    # set up a connection to the NKN network
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_host_keys(os.path.expanduser(
        os.path.join("~", ".ssh", "known_hosts")))
    ssh.connect('reacchdb.nkn.uidaho.edu', username='vicmet', password='cl!m@te')
    sftp = ssh.open_sftp()

    # transfer the percentile files
    if section == 'MONITOR':
        varnames = ['swe', 'sm', 'tm']
        for var in varnames:
            if date[-2:] == '01':
                sftp.put(
                    os.path.join(
                        source_loc, 'vic-metdata_%spercentile_%s.nc' %
                        (var, date)), os.path.join(
                            dest_loc, 'vic-metdata_%spercentile_%s.nc' %
                            (var, date)))
            sftp.put(os.path.join(
                source_loc, 'vic-metdata_%spercentile_%s.nc' % (var, date)),
                     os.path.join(dest_loc, 'vic-metdata_%spercentile_CURRENT.nc' %
                                  (var)))
            print(os.path.join(source_loc, 'vic-metdata_%spercentile_%s.nc' %
                               (var, date)))
        var = 'ro'
        for agg in ['7d', '15d', '30d', '60d', '90d', 'ccy', 'cwy']:
            if date[-2:] == '01':
                sftp.put(
                    os.path.join(
                        source_loc, 'vic-metdata_{0}percentile_{1}_{2}.nc'.format(
                            var, agg, date)), os.path.join(
                                dest_loc,
                                'vic-metdata_{0}percentile_{1}_{2}{3}.nc'.format(
                                    var, agg, month, str(1))))
            sftp.put(
                os.path.join(
                    source_loc, 'vic-metdata_{0}percentile_{1}_{2}.nc'.format(
                        var, agg, date)), os.path.join(
                            dest_loc, 'vic-metdata_{0}percentile_{1}.nc'.format(
                                var, agg)))
            print(os.path.join(
                source_loc, 'vic-metdata_{0}percentile_{1}_{2}.nc'.format(
                    var, agg, date)))

        # create a .txt file to include the date
        fout = sftp.open(os.path.join(dest_loc, 'lastDate.txt'), 'w+')
        fout.write('%s/%s/%s' % (date[:4], date[5:7], date[8:10]))
        fout.close()
        print(os.path.join(dest_loc, 'lastDate.txt'))
    elif section == 'MED_FCST':
        forecast_date = current_date + timedelta(days=1)
        print('medium range forecast')
        print(forecast_date)
        varnames = ['swe', 'sm', 'tm', 'ro']
        for agg in ['1', '2', '3', '4', '12', '123', '1234']:
            for var in varnames:
                sftp.put(os.path.join(source_loc, 'vic-CFSv2_wk{}_{}percentile_{}.nc'.format(
                                      agg, var, str(forecast_date)[0:10])), os.path.join(dest_loc,
                                       'vic-CFSv2_wk{}_{}percentile.nc'.format(
                                      agg, var)))
        fout = sftp.open(os.path.join(dest_loc, 'lastDate.txt'), 'w+')
        fout.write('%s/%s/%s' % (str(forecast_date)[:4], str(forecast_date)[5:7], str(forecast_date)[8:10]))
        fout.close()
        print(os.path.join(dest_loc, 'lastDate.txt'))
    elif section == 'SEAS_FCST':
        file_creation_date = current_date + timedelta(days=1)
        forecast_date = current_date + timedelta(days=31)
        print('seasonal range forecast')
        print(forecast_date)
        # select the next six months going into the future which the forecast covers
        months = pd.date_range(forecast_date, periods=7, freq='M')
        varnames = ['swe', 'sm', 'tm', 'ro']
        for (i, month) in enumerate(months):
            month_stamp = str(month)[0:7]
            for var in varnames:
                sftp.put(os.path.join(source_loc, 'vic-NMME_{}_{}percentile_{}.nc'.format(
                                      month_stamp, var, str(file_creation_date)[0:10])), os.path.join(dest_loc,
                 #                      'vic-NMME_{}_{}percentile_{}.nc'.format(
                   #                   month_stamp, var, str(file_creation_date)[0:7])))
                 'vic_bcsdNMME_forecast_{}percentile_{}MonthLead_1MosAvgs.nc'.format(
                                      var, str(i+1))))
        fout = sftp.open(os.path.join(dest_loc, 'lastDate.txt'), 'w+')
        fout.write('%s/%s' % (str(forecast_date)[:4], str(forecast_date)[5:7]))
        fout.close()
        print(os.path.join(dest_loc, 'lastDate.txt'))
    sftp.close()
    ssh.close()


if __name__ == "__main__":
    main()
