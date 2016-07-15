Monitor
=======
`monitor` is a near real-time monitor of environmental conditions,
designed to be run through the work flow package ecFlow.

## Overview:
Monitor has four main components.

**Meteorological**  `/monitor/tools/bin/ecflow/processes/main/meteorological/`
	Contains scripts for the manipulation of meteorological data, 
including to download, regrid, and file format conversion.
Uses `tonic` to convert netcdf to ascii.

**Models**  `/monitor/tools/bin/ecflow/processes/main/models/`
	Contains scripts to conduct model runs. Initial implementation
uses the model `VIC`.

**Post-Processing**  `/monitor/tools/bin/ecflow/processes/main/post_processing/`
	Contains scripts to post-process model output, including file
format conversation, analysis, and plotting. Uses `tonic` to convert ascii to
netcdf 

**CDF Creator**  `/monitor/tools/cdf_creator/`
	Contains scripts to analyze historic model runs and extract cdfs for 
every day of the year to be used during `post_processing`.

## Requirements:  
- python 2.7 (ecflow dependency)
- numpy
- scipy
- matplotlib
- pandas
- xarray
- cartopy
- netCDF4
- configobj
- basemap
