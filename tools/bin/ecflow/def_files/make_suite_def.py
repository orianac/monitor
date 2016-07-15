#!/usr/bin/env python
"""
make_suite_def.py
"""
import os
import argparse

import ecflow

from tonic.io import read_config
from monitor.log import set_logger


def main():
    """
    Write suite definition file for drought monitor data processing.
    """

    logger = set_logger(os.path.splitext(os.path.split(__file__)[-1])[0],
                        'INFO')
    parser = argparse.ArgumentParser(
        description='Create suite definition file')
    parser.add_argument('suite_config_file', metavar='suite_config_file',
                        type=argparse.FileType('r'), nargs=1,
                        help='configuration file for suite definition file')
    args = parser.parse_args()
    suite_dict = read_config(args.suite_config_file[0].name)
    logger.info('Creating suite definition')
    defs = ecflow.Defs()

    for s in range(suite_dict['MAIN']['NSuites']):
        skey = 'S{}'.format(s)
        suite = defs.add_suite(suite_dict[skey]['Name'])
        suite = add_dependencies(suite, skey, suite_dict)
        suite.add_variable('SYS_PATH', os.environ['PATH'])

        for f in range(suite_dict[skey]['NFamilies']):
            fkey = 'S{0}_F{1}'.format(s, f)
            family = suite.add_family(suite_dict[fkey]['Name'])
            family = add_dependencies(family, fkey, suite_dict)
            for t in range(suite_dict[fkey]['NTasks']):
                tkey = 'S{0}_F{1}_T{2}'.format(s, f, t)
                task = family.add_task(suite_dict[tkey]['Name'])
                task = add_dependencies(task, tkey, suite_dict)
            for ff in range(suite_dict[fkey]['NFamilies']):
                ffkey = 'S{0}_F{1}_F{2}'.format(s, f, ff)
                ffamily = family.add_family(suite_dict[ffkey]['Name'])
                ffamily = add_dependencies(ffamily, ffkey, suite_dict)
                for t in range(suite_dict[ffkey]['NTasks']):
                    tkey = 'S{0}_F{1}_F{2}_T{3}'.format(s, f, ff, t)
                    task = ffamily.add_task(suite_dict[tkey]['Name'])
                    task = add_dependencies(task, tkey, suite_dict)

    logger.info(defs)
    if suite_dict['MAIN']['MakeDummyScripts']:
        defs.generate_scripts()

    if defs.check_job_creation():
        logger.critical(defs.check_job_creation())
    else:
        logger.info('Successful job creation: .ecf -> .job0')

    if len(defs.check()) != 0:
        logger.critical(defs.check())
    else:
        logger.info('Successful trigger expressions')

    logger.info('Saving definition to file {}'.format(
        suite_dict['MAIN']['DefFile']))
    defs.save_as_defs(suite_dict['MAIN']['DefFile'])
    return


def add_dependencies(node, key, suite_dict):
    # add optional meters and events
    if 'NEvents' in suite_dict[key]:
        for e in range(suite_dict[key]['NEvents']):
            node.add_event(suite_dict[key]['Event{}'.format(e)])
    if 'NMeters' in suite_dict[key]:
        for mt in range(suite_dict[key]['NMeters']):
            node.add_event(suite_dict[key]['Meter{}'.format(mt)])
    # add optional time-related dependencies
    if 'RepeatDay' in suite_dict[key]:
        node.add_repeat(ecflow.RepeatDay(suite_dict[key]['RepeatDay']))
    if 'NTimes' in suite_dict[key]:
        for n in range(suite_dict[key]['NTimes']):
            node.add_time(suite_dict[key]['Time{}'.format(n)])
    if 'NDates' in suite_dict[key]:
        for a in range(suite_dict[key]['NDates']):
            day = suite_dict[key]['DateDay{}'.format(a)]
            month = suite_dict[key]['DateMonth{}'.format(a)]
            year = suite_dict[key]['DateYear{}'.format(a)]
            node.add_date(day, month, year)
    if 'NDays' in suite_dict[key]:
        for b in range(suite_dict[key]['NDays']):
            node.add_day(suite_dict[key]['Day{}'.format(b)])
    # add optional trigger and complete dependencies
    if 'Trigger' in suite_dict[key]:
        node.add_trigger(suite_dict[key]['Trigger'])
    if 'Complete' in suite_dict[key]:
        node.add_complete(suite_dict[key]['Complete'])
    # add optional task-level variables
    if 'NVariables' in suite_dict[key]:
        for v in range(suite_dict[key]['NVariables']):
            node.add_variable(suite_dict[key]['VariableName{}'.format(v)],
                              suite_dict[key]['VariableValue{}'.format(v)])
    return node

if __name__ == "__main__":
    main()
