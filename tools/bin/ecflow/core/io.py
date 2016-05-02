from collections import namedtuple
import pandas as pd


def read_ghcnd_stations(station_file):
    '''Get station information. This is a fixed width file with fields defined
       in the ghcnd readme.txt file. Each tuple in col_specification contains
       the beginning (start) and end character position of a field. The
       respective field name is in the list names in the same order. These are
       used for the pandas read_fwf command.'''

    Field = namedtuple('Field', ['start', 'end'], verbose=False)

    col_specification = [Field(0, 11), Field(12, 20), Field(21, 30),
                         Field(31, 37), Field(38, 40), Field(41, 71),
                         Field(72, 75), Field(76, 79), Field(80, 85)]
    names = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATE', 'NAME',
             'GSN FLAG', 'HCN/CRN FLAG', 'WMO ID']
    inventory = pd.read_fwf(station_file, names=names,
                            colspecs=col_specification, na_values=(-9999),
                            index_col=0)
    return inventory


def read_used_stn_list(used_stn_list):
    inventory = pd.read_csv(used_stn_list, sep=' ', skipinitialspace=True,
                            index_col=0)
    return inventory


def get_full_stn_info(ghcnd_stn_file, used_stn_list):
    full_inventory = read_ghcnd_stations(ghcnd_stn_file)
    full_inv = full_inventory[['LONGITUDE', 'LATITUDE', 'ELEVATION', 'STATE',
                               'NAME']]
    full_inv.dropna()
    inventory = read_used_stn_list(used_stn_list)
    inv_df = full_inv.ix[inventory.index]
    return inv_df
