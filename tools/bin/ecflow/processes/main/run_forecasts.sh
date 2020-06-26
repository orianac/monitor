#!/bin/bash
#export PATH=/pool0/data/orianac/toolbox_from_hyak/miniconda2/bin:$PATH
#export PATH=/pool0/data/orianac/toolbox_from_hyak/miniconda2/envs/ecflow_env/bin:/pool0/data/orianac/toolbox_from_hyak/miniconda2/bin:$PATH
for number in 1;
do

scriptdir='/pool0/home/orianac/scripts/monitor/tools/bin/ecflow/processes/main/'
configdir='/pool0/data/orianac/climate_toolbox/monitor/config/'
# for the daily run
date >> /pool0/home/orianac/scripts/monitor/tools/bin/ecflow/processes/main/dates.txt
# run the get_metdata
source activate climate_toolbox #cdo_toolbox

which python

sed "s/DAYS_BEHIND/${number}/" ${configdir}/python_US_template.cfg > ${configdir}/python_US.cfg

# run get_metstate

python ${scriptdir}/meteorological/get_med_metfcst.py ${configdir}/working_python_US.cfg

python ${scriptdir}/meteorological/run_metsim_metgrid.py ${configdir}/working_python_US.cfg MED_FCST
python ${scriptdir}/models/prep_and_run_vic.py ${configdir}/working_python_US.cfg MED_FCST

python ${scriptdir}/post_processing/merge_forecasts.py ${configdir}/working_python_US.cfg MED_FCST
python ${scriptdir}/post_processing/analysis_forecasts.py ${configdir}/working_python_US.cfg MED_FCST
python ${scriptdir}/post_processing/file_transfer.py ${configdir}/working_python_US.cfg MED_FCST
# run metsim_metgrid - this works!!

#source activate climate_toolbox
done
