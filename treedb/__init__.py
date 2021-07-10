"""Load Glottolog lanuoid tree ``md.ini`` files into SQLite3 database."""

from ._globals import SESSION as Session

from ._tools import sha256sum

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
                             Producer,
                             Config)

from .backend.pandas import (pd_read_sql,
                             pd_read_json_lines)

from .backend.sqlite_master import (print_table_sql,
                                    select_tables_nrows)

from .backend.views import TABLES as views

from .languoids import set_root, iterfiles

from .checks import (check,
                     compare_languoids)

from .export import (print_languoid_stats,
                     iterlanguoids,
                     checksum,
                     write_json_lines as write_languoids,
                     pd_read_languoids,
                     write_files)

from .glottolog import checkout_or_clone

from .logging_ import configure_logging

from .models import (LEVEL,
                     Languoid)

from .queries import (get_example_query,
                      get_json_query as get_languoids_query,
                      iterdescendants)

from .settings import (configure,
                       get_default_root)


__all__ = ['Session',
           'sha256sum',
           'print_versions',
           'set_engine', 'connect', 'scalar', 'iterrows',
           'print_dataset',
           'print_schema', 'print_query_sql',
           'backup', 'dump_sql', 'csv_zipfile',
           'print_rows', 'write_csv', 'hash_csv',
           'load',
           'Dataset', 'Producer', 'Config',
           'pd_read_sql', 'pd_read_json_lines',
           'print_table_sql', 'select_tables_nrows',
           'views',
           'set_root', 'iterfiles',
           'check', 'compare_languoids',
           'print_languoid_stats',
           'iterlanguoids',
           'checksum',
           'write_languoids',
           'pd_read_languoids',
           'write_files',
           'checkout_or_clone',
           'configure_logging',
           'LEVEL', 'Languoid',
           'get_example_query',
           'get_languoids_query',
           'iterdescendants',
           'configure',
           'engine', 'root']

__title__ = 'treedb'
__version__ = '2.3.1.dev0'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2017-2021 Sebastian Bank'


# default engine: in-memory database
engine = set_engine(None, title=__title__)


# default root: GLOTTOLOG_REPO_ROOT, or treedb.ini glottolog:repo_root, or ./glottolog
root = set_root(get_default_root(env_var='GLOTTOLOG_REPO_ROOT'))
