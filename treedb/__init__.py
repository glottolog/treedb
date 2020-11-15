# treedb - load glottolog/languoids/tree/**/md.ini into sqlite3

"""Load Glottolog lanuoid tree md.ini files into SQLite3 database."""

CONFIG = 'treedb.ini'

DEFAULT_ROOT = './glottolog/'

from . import proxies as _proxies

ENGINE = _proxies.SQLiteEngineProxy()

ROOT = _proxies.PathProxy()

from .config import configure, get_default_root

from .logging_ import configure_logging

from .glottolog import checkout_or_clone

from .files import set_root, iterfiles

from .languoids import iterlanguoids, compare_with_files, write_files

from .backend import (print_query_sql, set_engine, print_schema,
                      Dataset, Producer, Session,
                      backup, dump_sql, export)

from .backend_load import load

from .sqlite_master import print_table_sql, select_tables_nrows

from .languoids_json import write_json_csv, checksum

from .models import LEVEL, Languoid

from .checks import check

from .queries import (print_rows, write_csv, hash_csv,
                      get_query,
                      write_json_query_csv, write_json_lines, get_json_query,
                      print_languoid_stats,
                      iterdescendants)

from .shortcuts import (count, select, text,
                        pd_read_sql)

__all__ = ['ENGINE', 'ROOT',
           'configure', 'configure_logging',
           'checkout_or_clone',
           'set_root', 'iterfiles',
           'iterlanguoids', 'compare_with_files', 'write_files',
           'print_query_sql', 'set_engine', 'print_schema',
           'Dataset', 'Producer', 'Session',
           'backup', 'dump_sql', 'export',
           'load',
           'print_table_sql', 'select_tables_nrows',
           'write_json_csv', 'checksum',
           'LEVEL', 'Languoid',
           'check',
           'print_rows', 'write_csv', 'hash_csv',
           'get_query',
           'write_json_query_csv', 'write_json_lines', 'get_json_query',
           'print_languoid_stats',
           'iterdescendants',
           'count', 'select', 'text',
           'pd_read_sql',
           'engine', 'root']

__title__ = 'treedb'
__version__ = '1.3.5.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2020 Sebastian Bank'


# default engine: in-memory database
engine = set_engine(None, title=__title__)


# default root: GLOTTOLOG_REPO_ROOT, or treedb.ini glottolog:repo_root, or ./glottolog
root = set_root(get_default_root(env_var='GLOTTOLOG_REPO_ROOT',
                                 config_path=CONFIG,
                                 fallback=DEFAULT_ROOT))
