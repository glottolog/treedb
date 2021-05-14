"""Python 3.6 compatibility backports."""

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

    def datetime_fromisoformat(date_string: str):
        """Return datetime.datetime from isoformat string.

        >>> datetime_fromisoformat('2000-01-01T12:59:30')
        datetime.datetime(2000, 1, 1, 12, 59, 30)

        >>> datetime_fromisoformat('2000-01-01 12:59:30')
        datetime.datetime(2000, 1, 1, 12, 59, 30)

        >>> datetime_fromisoformat('2000-01-01T12:59:30.000000')
        datetime.datetime(2000, 1, 1, 12, 59, 30)

        >>> datetime_fromisoformat('2000-01-01 12:59:30.000000')
        datetime.datetime(2000, 1, 1, 12, 59, 30)

        >>> datetime_fromisoformat('nondatetime')
        Traceback (most recent call last):
            ...
        RuntimeError: cannot match time data: nondatetime
        """
        for f in _formats:
            try:
                return datetime.datetime.strptime(date_string, f)
            except ValueError as e:
                warnings.warn(f'date_string {date_string!r} failed'
                              f' datetime_fromisoformat: {e}')
        else:
            raise RuntimeError(f'cannot match time data: {date_string}')

    datetime_toisoformat = operator.methodcaller('strftime',
                                                 DATETIME_FORMAT)

else:
    from contextlib import nullcontext
    import operator

    datetime_fromisoformat = datetime.datetime.fromisoformat

    assert (datetime.datetime(2000, 1, 1).isoformat()
            == datetime.datetime(2000, 1, 1).strftime(DATETIME_FORMAT))
    datetime_toisoformat = operator.methodcaller('isoformat')
