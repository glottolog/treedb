# treedb - load glottolog/languoids/tree/**/md.ini into sqlite3

"""Load Glottolog lanuoid tree md.ini files into SQLite3 database."""

CONFIG = 'treedb.ini'

DEFAULT_ROOT = './glottolog/'

from ._basics import (ENGINE, ROOT, REGISTRY,
                      SESSION as Session)

from .backend import set_engine, scalar, iterrows, connect

from .backend.export import backup, dump_sql, csv_zipfile as export

from .backend.load import load

from .backend.models import Dataset, Producer

from .backend.sqlite_master import print_table_sql, select_tables_nrows

from .backend.tools import print_query_sql, print_schema

from .checks import check

from .config import configure, get_default_root

from .files import set_root, iterfiles

from .glottolog import checkout_or_clone

from .languoids import iterlanguoids, compare_with_files, write_files

from .languoids_json import write_json_csv, checksum

from .logging_ import configure_logging

from .models import LEVEL, Languoid

from .queries import (print_rows, write_csv, hash_csv,
                      get_query,
                      write_json_query_csv, write_json_lines, get_json_query,
                      print_languoid_stats,
                      iterdescendants)

from .shortcuts import pd_read_sql

__all__ = ['ENGINE', 'ROOT', 'REGISTRY', 'Session',
           'set_engine', 'scalar', 'iterrows', 'connect',
           'backup', 'dump_sql', 'export',
           'load',
           'Dataset', 'Producer',
           'print_table_sql', 'select_tables_nrows',
           'print_query_sql', 'print_schema',
           'check',
           'configure',
           'set_root', 'iterfiles',
           'checkout_or_clone',
           'iterlanguoids', 'compare_with_files', 'write_files',
           'write_json_csv', 'checksum',
           'configure_logging',
           'LEVEL', 'Languoid',
           'print_rows', 'write_csv', 'hash_csv',
           'get_query',
           'write_json_query_csv', 'write_json_lines', 'get_json_query',
           'print_languoid_stats',
           'iterdescendants',
           'pd_read_sql',
           'engine', 'root']

__title__ = 'treedb'
__version__ = '1.5.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2021 Sebastian Bank'


# default engine: in-memory database
engine = set_engine(None, title=__title__)


# default root: GLOTTOLOG_REPO_ROOT, or treedb.ini glottolog:repo_root, or ./glottolog
root = set_root(get_default_root(env_var='GLOTTOLOG_REPO_ROOT',
                                 config_path=CONFIG,
                                 fallback=DEFAULT_ROOT))
