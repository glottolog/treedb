# treedb - load glottolog/languoids/tree/**/md.ini into sqlite3

"""Load Glottolog lanuoid tree md.ini files into SQLite3 database."""

CONFIG = 'treedb.ini'

DEFAULT_ROOT = '.'

from . import proxies as _proxies

ROOT, ENGINE = _proxies.PathProxy(), _proxies.SQLiteEngineProxy()

from .config import configure, get_default_root

from .logging import configure_logging

from .files import set_root, iterfiles

from .languoids import (iterlanguoids,
                        compare_with_files,
                        write_files)

from .languoids_json import write_json_csv, checksum

from .backend import (print_query_sql, set_engine, print_schema,
                      Dataset, Producer, Session,
                      backup, dump_sql, export,
                      print_table_sql, select_stats)

from .backend_load import load

from .models import LEVEL, Languoid

from .checks import check

from .queries import (print_rows, write_csv, hash_csv,
                      get_query,
                      write_json_query_csv, get_json_query,
                      print_languoid_stats,
                      iterdescendants)

from .shortcuts import (count, select, text,
                        pd_read_sql)

__all__ = ['ROOT', 'ENGINE',
           'configure', 'configure_logging',
           'set_root', 'iterfiles',
           'iterlanguoids', 'compare_with_files', 'write_files',
           'write_json_csv', 'checksum',
           'print_query_sql', 'set_engine', 'print_schema',
           'Dataset', 'Producer', 'Session',
           'backup', 'dump_sql', 'export',
           'print_table_sql',  'select_stats',
           'load',
           'LEVEL', 'Languoid',
           'check',
           'print_rows', 'write_csv', 'hash_csv',
           'get_query',
           'write_json_query_csv', 'get_json_query',
           'print_languoid_stats',
           'iterdescendants',
           'count', 'select', 'text',
           'pd_read_sql',
           'root', 'engine']

__title__ = 'treedb'
__version__ = '0.8.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2020 Sebastian Bank'

# default root: GLOTTOLOG_REPO_ROOT, or treedb.ini glottolog/repo_root, or cwd
root = set_root(get_default_root(env_var='GLOTTOLOG_REPO_ROOT',
                                 config_path=CONFIG,
                                 fallback=DEFAULT_ROOT))

# default engine: in-memory database
engine = set_engine(None, title=__title__)
