# _compat.py - Python 2/3 compatibility

import sys
import operator

PY2 = (sys.version_info.major == 2)


if PY2:
    import pathlib2 as pathlib
    
    from scandir import scandir

    iteritems = operator.methodcaller('iteritems')


else:
    import pathlib
    
    from os import scandir

    def iteritems(d):
        return iter(d.items())
