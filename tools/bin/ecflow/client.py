#!/usr/bin/env python2.7

import ecflow

try:
    print "Loading definition in 'suite.def' into the server"
    ci = ecflow.Client()
    ci.delete_all(force=True)
    ci.restart_server()
    ci.load("suite.def")   # read definition form disk and load into the server
    ci.begin_suite("processes")
except RuntimeError as e:
    print "Failed:", e
