# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from . _compat import pathlib

from .languoids import iterlanguoids
from .backend import engine, Session, print_rows
from .models import Languoid, load, check, get_query, iterdescendants

from . import values

from . import files as _files
from . import backend as _backend

__all__ = [
    'ROOT',
    'iterlanguoids',
    'engine', 'Session', 'print_rows',
    'Languoid', 'load', 'check', 'get_query', 'iterdescendants',
    'files_roundtrip',
    'export_db', 'write_csv',
]

__title__ = 'treedb'
__version__ = '0.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2019 Sebastian Bank'

_PACKAGE_DIR = pathlib.Path(__file__).parent

ROOT = _PACKAGE_DIR / '../../glottolog/languoids/tree'


def files_roundtrip(verbose=False):
    """Do a load/save cycle with all config files."""
    def _iterpairs(triples):
        for path_tuple, _, cfg in triples:
            d = {s: dict(cfg.items(s)) for s in cfg.sections()}
            yield path_tuple, d

    pairs = _iterpairs(_files.iterconfig())
    _files.save(pairs, assume_changed=True, verbose=verbose)


def export_db():
    """Dump .sqlite file to a ZIP file with one CSV per table, return filename."""
    return _backend.export()


def write_csv(query=None, filename='treedb.csv', encoding='utf-8'):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()
    return _backend.write_csv(query, filename, encoding=encoding)
