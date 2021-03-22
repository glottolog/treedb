# Python 3.6 compatibility backports

import datetime
import operator
import sys
import warnings

__all__ = ['nullcontext',
           'datetime_fromisoformat']

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


if sys.version_info < (3, 7):
    import contextlib

    @contextlib.contextmanager
    def nullcontext(enter_result=None):
        yield enter_result

    _formats = (DATETIME_FORMAT,
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f')

    def datetime_fromisoformat(date_string):
        result = None
        try:
            for f in _formats:
                result = datetime.datetime.strptime(date_string, f)
                break
            else:
                raise RuntimeError(f'cannot match time data: {date_string}')
        except ValueError as e:
            warnings.warn(f'date_string {date_string!r} failed'
                          f' datetime_fromisoformat: {e}')
        else:
            return result

    datetime_toisoformat = operator.methodcaller('strftime',
                                                 DATETIME_FORMAT)

else:
    from contextlib import nullcontext
    import operator

    datetime_fromisoformat = datetime.datetime.fromisoformat

    assert (datetime.datetime(2000, 1, 1).isoformat()
            == datetime.datetime(2000, 1, 1).strftime(DATETIME_FORMAT))
    datetime_toisoformat = operator.methodcaller('isoformat')
