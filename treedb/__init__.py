# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from . import proxies as _proxies

ROOT, ENGINE = _proxies.PathProxy(), _proxies.SQLiteEngineProxy()

from .files import get_default_root, set_root, iterfiles
from .languoids import iterlanguoids, to_json_csv, compare_with_raw
from .backend import (create_engine, load, Dataset, Session,
                      print_schema, dump_sql, export)
from .models import LEVEL, Languoid
from .checks import check
from .queries import print_rows, write_csv, hash_csv, get_query, iterdescendants
from .sa_helpers import text, select, count

__all__ = [
    'ROOT', 'ENGINE',
    'set_root', 'iterfiles',
    'iterlanguoids', 'to_json_csv', 'compare_with_raw',
    'create_engine', 'load', 'Dataset', 'Session',
    'print_schema', 'dump_sql', 'export',
    'LEVEL', 'Languoid',
    'check',
    'print_rows', 'write_csv', 'hash_csv', 'get_query', 'iterdescendants',
    'text', 'select', 'count',
    'root', 'engine',
]

__title__ = 'treedb'
__version__ = '0.1.5'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2020 Sebastian Bank'

# default root: TREEDB_REPO or repo_root in sister git checkout or cwd
root = set_root(get_default_root('TREEDB_REPO', '../glottolog', '.'))

# default engine: in-memory database
engine = create_engine(None, title=__title__)
