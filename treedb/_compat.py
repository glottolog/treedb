# _compat.py - Python 3.6 compatibility backports

import sys

import datetime

['datetime_fromisoformat']


if sys.version_info >= (3, 7):
    datetime_fromisoformat = datetime.datetime.fromisoformat

else:
    _format = '%Y-%m-%d %H:%M:%S.%f'

    def datetime_fromisoformat(date_string):
        return datetime.datetime.strptime(date_string, _format)
