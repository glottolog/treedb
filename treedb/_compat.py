# _compat.py - Python 3.6 compatibility backports

import datetime
import sys

__all__ = ['nullcontext',
           'datetime_fromisoformat']


if sys.version_info < (3, 7):
    import contextlib

    @contextlib.contextmanager
    def nullcontext(enter_result=None):
        yield enter_result

    _format = '%Y-%m-%d %H:%M:%S.%f'

    def datetime_fromisoformat(date_string):
        return datetime.datetime.strptime(date_string, _format)

else:
    from contextlib import nullcontext

    datetime_fromisoformat = datetime.datetime.fromisoformat
