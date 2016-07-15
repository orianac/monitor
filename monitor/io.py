'''
io.py
'''

import os
import subprocess
import numpy as np
from tempfile import mkstemp
from shutil import move, rmtree


def proc_subprocess(executing_arguments, log_path):
    proc = subprocess.Popen(' '.join(executing_arguments),
                            shell=True,
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    retvals = proc.communicate()

    stdout = retvals[0]
    stderr = retvals[1]
    returncode = proc.returncode

    with open(os.path.join(log_path, 'log_file.txt'), "a") as logfile:
        logfile.write(stderr + stdout)
