"""
compat.py
"""

import sys

if sys.version_info[0] >= 3:
    import copyreg as copy_reg
else:
    import copy_reg
