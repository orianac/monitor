#! /usr/bin/env python
# usage: <python> <setup.py> <install>
import os
# temporarily redirect config directory to prevent matplotlib importing
# testing that for writeable directory which results in sandbox error in
# certain easy_install versions
os.environ["MPLCONFIGDIR"] = "."

DESCRIPTION = "Monitor: PNW Drought Monitor"
LONG_DESCRIPTION = """\
Downloading meteorological data from UIdaho
and after processing that data is used to force VIC
and then those results are compared with 30 years of historic data.
"""

DISTNAME = 'monitor'
MAINTAINER = 'Hordur Helgason'
MAINTAINER_EMAIL = 'helgason@uw.edu'
URL = 'unknown'
LICENSE = 'GNU GENERAL PUBLIC LICENSE Version 3, 29'
DOWNLOAD_URL = 'https://github.com/UW-Hydro/monitor'
VERSION = '0.0.0'

try:
    from setuptools import setup
    _has_setuptools = True
except ImportError:
    from distutils.core import setup


def check_dependencies():
    install_requires = []

    # Just make sure dependencies exist
    try:
        import numpy
    except ImportError:
        install_requires.append('numpy')
    try:
        import scipy
    except ImportError:
        install_requires.append('scipy')
    try:
        import matplotlib
    except ImportError:
        install_requires.append('matplotlib')
    try:
        import pandas
    except ImportError:
        install_requires.append('pandas')
    try:
        import xarray
    except ImportError:
        install_requires.append('xarray')
    try:
        import cartopy
    except ImportError:
        install_requires.append('cartopy')
    try:
        import netCDF4
    except ImportError:
        install_requires.append('netCDF4')
    try:
        import configobj
    except:
        install_requires.append('configobj')

    return install_requires

if __name__ == "__main__":

    install_requires = check_dependencies()

    setup(name=DISTNAME,
          author=MAINTAINER,
          author_email=MAINTAINER_EMAIL,
          maintainer=MAINTAINER,
          maintainer_email=MAINTAINER_EMAIL,
          description=DESCRIPTION,
          long_description=LONG_DESCRIPTION,
          license=LICENSE,
          url=URL,
          version=VERSION,
          download_url=DOWNLOAD_URL,
          install_requires=install_requires,
          packages=['monitor'])
