'''
model_tools.py
'''

import os.path
from .os_tools import file_chmod


def copy_clean_vic_config(src, dst, header=None, **kwargs):
    ''' From Joe Hamman's build_vic_namelistCopy. VIC style ASCII configuration
    file from src to dst. Remove comments and empty lines. Replace keywords
    in brackets with variable values in **kwargs dict. '''
    with open(src, 'r') as fsrc:
        with open(dst, 'w') as fdst:
            lines = fsrc.readlines()
            if header is not None:
                fdst.write(header)
            for line in lines:
                line = line.format(**kwargs)
                line = os.path.expandvars(line.split('#', 1)[0].strip())
                # new line is needed because strip() command removes new
                # line from end of line in order to remove blank lines
                if line:
                    fdst.write(line + '\n')
    file_chmod(dst)


def replace_var_pythonic_config(src, dst, header=None, **kwargs):
    ''' Python style ASCII configuration file from src to dst. Dost not remove
    comments or empty lines. Replace keywords in brackets with variable values
    in **kwargs dict. '''
    with open(src, 'r') as fsrc:
        with open(dst, 'w') as fdst:
            lines = fsrc.readlines()
            if header is not None:
                fdst.write(header)
            for line in lines:
                line = line.format(**kwargs)
                fdst.write(line)
    file_chmod(dst)
