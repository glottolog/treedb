# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from . import proxies as _proxies
from . import tools as _tools

ENGINE = _proxies.SqliteEngineProxy()

_PACKAGE_DIR = _tools.path_from_filename(__file__).parent

ROOT = _PACKAGE_DIR.parent.parent.joinpath('glottolog', 'languoids', 'tree')

from .files import iterfiles
from .languoids import iterlanguoids, to_json_csv, compare_with_raw
from .backend import Session, Dataset, load, export
from .models import LEVEL, Languoid
from .checks import check
from .queries import print_rows, write_csv, get_query, iterdescendants
from .sa_helpers import text, select, count

__all__ = [
    'ENGINE', 'ROOT',
    'iterfiles',
    'iterlanguoids', 'to_json_csv', 'compare_with_raw',
    'Session', 'Dataset', 'load', 'export',
    'LEVEL', 'Languoid',
    'check',
    'print_rows', 'write_csv', 'get_query', 'iterdescendants',
    'text', 'select', 'count',
    'set_engine_file',
]

__title__ = 'treedb'
__version__ = '0.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2019 Sebastian Bank'


def set_engine_file(filename, require=False, resolve=False):
    if require and filename is not None:
        path = _tools.path_from_filename(filename)
        if not path.exists():
            raise RuntimeError('engine file does not exist: %r' % path)

    ENGINE.__class__.file.fset(ENGINE, filename, resolve=resolve,
                               memory_filename='%s-memory.memory' % __title__)

    return ENGINE


set_engine_file(None)
