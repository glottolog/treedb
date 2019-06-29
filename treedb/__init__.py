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

from .main import (iterlanguoids,
                   engine, Languoid,
                   load,  check, export_db, write_csv,
                   get_query, iterdescendants)

__all__ = [
    'iterlanguoids',
    'engine',  'Languoid',
    'load',  'check', 'export_db', 'write_csv',
    'get_query',
    'iterdescendants',
]
