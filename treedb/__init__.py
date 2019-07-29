# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from __future__ import print_function

from . _compat import iteritems, zip_longest
from . _compat import pathlib

_PACKAGE_DIR = pathlib.Path(__file__).parent

FILE = pathlib.Path('treedb.sqlite3')

ROOT = _PACKAGE_DIR / '../../glottolog/languoids/tree'

ENCODING = 'utf-8'

from .files import iterconfig as iterfiles
from .languoids import iterlanguoids
from .backend import ENGINE as engine, Session, Dataset, load, print_rows
from .models import Languoid
from .checks import check
from .queries import get_query, iterdescendants

from . import files as _files
from . import backend as _backend

__all__ = [
    'ROOT',
    'iterfiles',
    'iterlanguoids',
    'engine', 'Session', 'Dataset', 'load', 'print_rows',
    'Languoid',
    'check',
    'get_query', 'iterdescendants',
    'files_roundtrip',
    'export_db', 'write_csv',
    'compare_with_raw',
]

__title__ = 'treedb'
__version__ = '0.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2019 Sebastian Bank'


def files_roundtrip(verbose=False):
    """Do a load/save cycle with all config files."""
    def _iterpairs(triples):
        for path_tuple, _, cfg in triples:
            d = {s: dict(m) for s, m in iteritems(cfg) if s != 'DEFAULT'}
            yield path_tuple, d

    pairs = _iterpairs(_files.iterconfig())
    _files.save(pairs, assume_changed=True, verbose=verbose)


def export_db():
    """Dump .sqlite file to a ZIP file with one CSV per table, return filename."""
    return _backend.export()


def write_csv(query=None, filename='treedb.csv', encoding=ENCODING):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()
    return _backend.write_csv(query, filename, encoding=encoding)


def compare_with_raw(root=ROOT):
    same = True
    for f, r in zip_longest(iterlanguoids(root), iterlanguoids(from_raw=True)):
        if f != r:
            same = False
            print('', '', f, '', r, '', '', sep='\n')
    return same
