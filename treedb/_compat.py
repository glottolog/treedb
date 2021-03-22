# Python 3.6 compatibility backports

import datetime
import sys
import warnings

__all__ = ['nullcontext',
           'datetime_fromisoformat']


if sys.version_info < (3, 7):
    import contextlib

    @contextlib.contextmanager
    def nullcontext(enter_result=None):
        yield enter_result

    _formats = ('%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S.%f')

    def datetime_fromisoformat(date_string):
        result = None
        try:
            for f in _formats:
                result = datetime.datetime.strptime(date_string, f)
                break
            else:
                raise RuntimeError(f'cannot match time data: {date_string}')
        except ValueError as e:
            warnings.warn(f'failed datetime_fromisoformat: {e}')
        else:
            return result

else:
    from contextlib import nullcontext

    datetime_fromisoformat = datetime.datetime.fromisoformat
