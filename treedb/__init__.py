# treedb - load ../../glottolog/languoids/tree/**/md.ini into sqlite3

"""Example session (glottolog cloned into the same directory as this repo)

$ python -m venv .venv  # PY3
$ source .venv/bin/activate  # Windows: $ .venv/Scripts/activate.bat
$ pip install -r treedb-requirements.txt

$ python
>>> import treedb
>>> next(treedb.iterlanguoids())
{'id': 'abin1243', 'parent_id': None, 'level': 'language', ...

>>> treedb.load()
...
'treedb.sqlite3'

>>> treedb.check()
...

>>> treedb.export_db()
'treedb.zip'

>>> treedb.write_csv()
'treedb.csv'

>>> treedb.load(rebuild=True)
...
'treedb.sqlite3'

>>> import sqlalchemy as sa
>>> treedb.write_csv(sa.select([treedb.Languoid]), filename='languoids.csv')

>>> sa.select([treedb.Languoid], bind=treedb.engine).execute().first()
('abin1243', 'language', 'Abinomn', None, 'bsa', 'bsa', -2.92281, 138.891)

>>> session = treedb.Session()
>>> session.query(treedb.Languoid).first()
<Languoid id='abin1243' level='language' name='Abinomn' hid='bsa' iso639_3='bsa'>
>>> session.close()
"""

from . _compat import pathlib

from . import backend as _backend
from .backend import engine, Session, print_rows
from .main import (iterlanguoids, Languoid, load, check,
                   get_query, iterdescendants)

__all__ = [
    'ROOT',
    'engine', 'Session', 'print_rows'
    'iterlanguoids', 'Languoid',
    'load', 'check', 'get_query', 'iterdescendants',
    'export_db', 'write_csv',
]

_PACKAGE_DIR = pathlib.Path(__file__).parent

ROOT = _PACKAGE_DIR / '../../glottolog/languoids/tree'


def export_db():
    """Dump .sqlite file to a ZIP file with one CSV per table, return filename."""
    return _backend.export()


def write_csv(query=None, filename='treedb.csv', encoding='utf-8'):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()
    return _backend.write_csv(query, filename, encoding=encoding)
