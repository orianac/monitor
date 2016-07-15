"""
share.py
"""

LOG_LEVEL = 'DEBUG'

# constants
KELVIN = 273.15

# Allow multiprocessing to pickle bound methods


def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)
