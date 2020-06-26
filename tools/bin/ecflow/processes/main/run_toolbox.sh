#!/bin/bash
#export PATH=/pool0/data/orianac/toolbox_from_hyak/miniconda2/bin:$PATH
#export PATH=/pool0/data/orianac/toolbox_from_hyak/miniconda2/envs/ecflow_env/bin:/pool0/data/orianac/toolbox_from_hyak/miniconda2/bin:$PATH
for number in 2;
do

scriptdir='/pool0/home/orianac/scripts/monitor/tools/bin/ecflow/processes/main/'
configdir='/pool0/data/orianac/climate_toolbox/monitor/config/'
# for the daily run
date >> /pool0/home/orianac/scripts/monitor/tools/bin/ecflow/processes/main/dates.txt
# run the get_metdata
source activate climate_toolbox #cdo_toolbox

which python

sed "s/DAYS_BEHIND/${number}/" ${configdir}/python_US_template.cfg > ${configdir}/python_US.cfg

python ${scriptdir}/meteorological/get_metdata.py ${configdir}/python_US.cfg
# run get_metstate

python ${scriptdir}/meteorological/get_metstate.py ${configdir}/working_python_US.cfg
####3#source activate /pool0/data/orianac/toolbox_from_hyak/miniconda2/envs/monitor_py3/

# run metsim_metgrid - this works!!

#which python

python ${scriptdir}/meteorological/run_metsim_metgrid.py ${configdir}/working_python_US.cfg MONITOR
#source activate climate_toolbox
# run run_vic - this works!!
python ${scriptdir}/models/prep_and_run_vic_twice.py ${configdir}/working_python_US.cfg MONITOR
# run percentiles - this works!! (TODO: merge)

which python
python ${scriptdir}/post_processing/merge_fluxes.py ${configdir}/working_python_US.cfg MONITOR
python ${scriptdir}/post_processing/storage_analysis.py ${configdir}/working_python_US.cfg MONITOR
python ${scriptdir}/post_processing/ro_analysis.py ${configdir}/working_python_US.cfg MONITOR

# run transfer (TODO: do tihs) 
python ${scriptdir}/post_processing/file_transfer.py  ${configdir}/working_python_US.cfg MONITOR
#source activate climate_toolbox
done
