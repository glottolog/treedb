# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from . import proxies as _proxies

ROOT, ENGINE = _proxies.PathProxy(), _proxies.SQLiteEngineProxy()

from .files import set_root, iterfiles
from .languoids import iterlanguoids, to_json_csv, compare_with_raw
from .backend import (create_engine, load, Dataset, Session,
                      print_schema, dump_sql, export)
from .models import LEVEL, Languoid
from .checks import check
from .queries import print_rows, write_csv, hash_csv, get_query, iterdescendants
from .sa_helpers import text, select, count

from . import tools as _tools

__all__ = [
    'iterfiles',
    'iterlanguoids', 'to_json_csv', 'compare_with_raw',
    'load', 'Dataset', 'Session',
    'print_schema', 'dump_sql', 'export',
    'LEVEL', 'Languoid',
    'check',
    'print_rows', 'write_csv', 'hash_csv', 'get_query', 'iterdescendants',
    'text', 'select', 'count',
    'ROOT', 'ENGINE', 'set_root', 'create_engine',
]

__title__ = 'treedb'
__version__ = '0.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'Apache License, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2019 Sebastian Bank'

_PACKAGE_DIR = _tools.path_from_filename(__file__).parent


set_root(_PACKAGE_DIR.parent.parent.joinpath('glottolog', 'languoids', 'tree'))


create_engine(None, title=__title__)
