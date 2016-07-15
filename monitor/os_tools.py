'''
os_tools.py
'''

from __future__ import print_function
import os
import os.path
import sys

from tonic.pycompat import basestring


def make_dirs(paths):
    ''' Make directories from list or string and change mode to 775. '''
    if isinstance(paths, basestring):
        if not os.path.exists(paths):
            try:
                os.makedirs(paths)
                dir_chmod(paths)
            except IOError:
                print('Could not make {0}'.format(paths))
    # Check iterable for stringness of all items. Will raise TypeError if
    # paths is not iterable
    elif hasattr(paths, '__iter__') and all(isinstance(p, basestring)
                                            for p in paths):
        for p in paths:
            if not os.path.exists(p):
                try:
                    os.makedirs(p)
                    dir_chmod(p)
                except IOError:
                    print('Could not make {0}'.format(paths))
    else:
        raise TypeError('paths must be string or a list of strings')


def dir_chmod(directory, mode='775'):
    '''Changes directory privileges with default of drwxrwxr-x. Convert mode
    from string to base-8  to be compatible with python 2 and python 3.'''
    os.chmod(directory, int(mode, 8))


def file_chmod(infile, mode='664'):
    '''Changes file privileges with default of -rw-rw-r--. Convert mode from
    string to base-8  to be compatible with python 2 and python 3.'''
    os.chmod(infile, int(mode, 8))
