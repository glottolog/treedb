# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Load glottolog lanuoid tree ini files into SQLite3 database."""

from . import proxies as _proxies

ROOT, ENGINE = _proxies.PathProxy(), _proxies.SQLiteEngineProxy()

from .files import get_default_root, set_root, iterfiles

from .languoids import (iterlanguoids,
                        compare_with_files,
                        write_files)

from .languoids_json import write_json_csv

from .backend import (set_engine, load, Dataset, Session,
                      print_schema, dump_sql, export, backup,
                      print_table_sql, print_query_sql, select_stats)

from .models import LEVEL, Languoid

from .checks import check

from .queries import (print_rows, write_csv, hash_csv,
                      get_query, get_json_query,
                      iterdescendants)

from .shortcuts import (count, select, text,
                        pd_read_sql,
                        configure_logging)

__all__ = ['ROOT', 'ENGINE',
           'set_root', 'iterfiles',
           'iterlanguoids', 'compare_with_files', 'write_files',
           'write_json_csv',
           'set_engine', 'load', 'Dataset', 'Session',
           'print_schema', 'dump_sql', 'export', 'backup',
           'print_table_sql', 'print_query_sql', 'select_stats',
           'LEVEL', 'Languoid',
           'check',
           'print_rows', 'write_csv', 'hash_csv',
           'get_query', 'get_json_query', 'iterdescendants',
           'count', 'select', 'text', 'pd_read_sql', 'configure_logging',
           'root', 'engine']

__title__ = 'treedb'
__version__ = '0.5.1'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2020 Sebastian Bank'

# default root: TREEDB_REPO or repo_root in sister git checkout or cwd
root = set_root(get_default_root('TREEDB_REPO', '../glottolog', '.'))

# default engine: in-memory database
engine = set_engine(None, title=__title__)
