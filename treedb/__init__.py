# load glottolog/languoids/tree/**/md.ini into sqlite3

"""Load Glottolog lanuoid tree md.ini files into SQLite3 database."""

from ._globals import SESSION as Session

from .backend import (print_versions,
                      set_engine,
                      connect,
                      scalar,
                      iterrows)

from .backend.export import (print_dataset,
                             print_schema,
                             print_query_sql,
                             backup,
                             dump_sql,
                             csv_zipfile,
                             print_rows,
                             write_csv,
                             hash_csv)

from .backend.load import main as load

from .backend.models import (Dataset,
                             Producer)

from .backend.sqlite_master import (print_table_sql,
                                    select_tables_nrows)

from .backend.views import TABLES as views

from .checks import (check,
                     compare_with_files)

from .config import (configure,
                     get_default_root)

from .export import (print_languoid_stats,
                     checksum,
                     write_json_csv,
                     write_json_query_csv,
                     write_json_lines,
                     write_files)

from .files import (set_root,
                    iterfiles)

from .glottolog import checkout_or_clone

from .languoids import iterlanguoids

from .logging_ import configure_logging

from .models import (LEVEL,
                     Languoid)

from .queries import (get_query,
                      get_json_query,
                      iterdescendants)

from .shortcuts import pd_read_sql

__all__ = ['Session',
           'print_versions',
           'set_engine', 'connect', 'scalar', 'iterrows',
           'print_dataset',
           'print_schema', 'print_query_sql',
           'backup', 'dump_sql', 'csv_zipfile',
           'print_rows', 'write_csv', 'hash_csv',
           'load',
           'Dataset', 'Producer',
           'print_table_sql', 'select_tables_nrows',
           'views',
           'check', 'compare_with_files',
           'configure',
           'print_languoid_stats',
           'checksum', 'write_json_csv',
           'write_json_query_csv', 'write_json_lines', 'write_files',
           'set_root', 'iterfiles',
           'checkout_or_clone',
           'iterlanguoids',
           'configure_logging',
           'LEVEL', 'Languoid',
           'get_query', 'get_json_query',
           'iterdescendants',
           'pd_read_sql',
           'engine', 'root']

__title__ = 'treedb'
__version__ = '1.5.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2021 Sebastian Bank'


# default engine: in-memory database
engine = set_engine(None, title=__title__)


# default root: GLOTTOLOG_REPO_ROOT, or treedb.ini glottolog:repo_root, or ./glottolog
root = set_root(get_default_root(env_var='GLOTTOLOG_REPO_ROOT'))
