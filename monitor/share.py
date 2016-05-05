"""
share.py
"""
import netCDF4
import sys

LOG_LEVEL = 'DEBUG'

# constants
SECONDS_PER_DAY = 3600*24
MM_PER_IN = 25.4
K_MIN = 273.15
#*******************************************
# Convert units kg/(m2-s) to mm/day
# multiply by (10^3mm m-1 x 86400 s day-1) and
# divide by density_H2O (1000 kg m-3):
# [kg/m2-s][1000 mm/m][86400 s/day][(1/1000) m3/kg] ==> mm/day
#*******************************************
KGM2S_TO_MMDAY = 86400.0 

# Set default netcdf fill values
NC_FILL_FLOAT = netCDF4.default_fillvals['f4']
NC_FILL_INT = netCDF4.default_fillvals['i4']
NC_FILL_STR = netCDF4.default_fillvals['S1']


# Allow multiprocessing to pickle bound methods
def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


if sys.version_info[0] >= 3:
    import copyreg as copy_reg
else:
    import copy_reg
