# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from __future__ import print_function

from . _compat import pathlib as _pathlib

_PACKAGE_DIR = _pathlib.Path(__file__).parent

ROOT = _PACKAGE_DIR.parent.parent.joinpath('glottolog', 'languoids', 'tree')

ENCODING = 'utf-8'

from .files import iterfiles
from .languoids import iterlanguoids, to_json_csv, compare_with_raw
from .backend import ENGINE, Session, Dataset, load, export
from .models import LEVEL, Languoid
from .checks import check
from .queries import print_rows, write_csv, get_query, iterdescendants
from .helpers import text, select, count, read_sql

FILE = _pathlib.Path.cwd() / 'treedb.sqlite3'

ENGINE.set_file(FILE)

__all__ = [
    'ROOT',
    'iterfiles',
    'iterlanguoids', 'to_json_csv', 'compare_with_raw',
    'Session', 'Dataset', 'load', 'export',
    'LEVEL', 'Languoid',
    'check',
    'print_rows', 'write_csv', 'get_query', 'iterdescendants',
    'text', 'select', 'count', 'read_sql',
    'FILE', 'engine'
]

__title__ = 'treedb'
__version__ = '0.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2019 Sebastian Bank'

engine = ENGINE
