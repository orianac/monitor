import os
import subprocess
import numpy as np
from tempfile import mkstemp
from shutil import move, rmtree

def proc_subprocess(executing_arguments, log_path):
    proc = subprocess.Popen( ' '.join(executing_arguments),
                            shell=True,
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE )
    retvals = proc.communicate()

    stdout = retvals[0]
    stderr = retvals[1]
    returncode = proc.returncode

    with open(os.path.join(log_path, 'log_file.txt') "a") as logfile:
        logfile.write( stderr+stdout )

def replace(file_path, pattern, subst):
    #Create temp file
    fh, abs_path = mkstemp()
    new_file = open(abs_path,'w')
    os.chmod(abs_path,0755)
    old_file = open(file_path)
    for line in old_file:
        new_file.write(line.replace(pattern, subst))
    #close temp file
    new_file.close()
    os.close(fh)
    old_file.close()
    #Remove original file
    os.remove(file_path)
    #Move new file
    move(abs_path, file_path)


