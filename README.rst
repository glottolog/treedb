Glottolog ``treedb``
====================

|PyPI version| |License| |Supported Python| |Format|

|Travis| |Codecov|

This tool loads the content of the `languoids/tree`_ directory from the
Glottolog_ `master repo`_ into a normalized SQLite_ database.

Each file under in that directory contains the definition of one Glottolog
languoid_. Loading their content into a relational database allows to perform
some advanced consistency checks (example_) and in general to execute queries
that inspect the languoid tree relations in a compact and performant way (e.g.
without repeatedly traversing the directory tree).

See pyglottolog_ for the more general official Python API to work with the repo
without a mandatory initial loading step (also provides programmatic access to
the references_ and a convenient command-line interface).

The database can be exported into a ZIP file containing one CSV file for
each database table, or written into a single denormalized CSV file with one
row per languoid (via a provided `SQL query`_).

As sqlite_ is the `most widely used`_ database, the database file itself
(e.g. ``treedb.sqlite3``) can be queried directly from most programming
environments. It can also be examined using graphical interfaces such as
DBeaver_, or via the `sqlite3 cli`_.

Python users can also use the provided SQLAlchemy_ models_ to build queries or
additional abstractions programmatically using `SQLAlchemy core`_ or the ORM_
(as more maintainable alternative to hand-written SQL queries).


Links
-----

- GitHub: https://github.com/glottolog/treedb
- PyPI: https://pypi.org/project/treedb/
- Example: https://nbviewer.jupyter.org/github/glottolog/treedb/blob/master/Stats.ipynb
- Changelog: https://github.com/glottolog/treedb/blob/master/CHANGES.txt
- Issue Tracker: https://github.com/glottolog/treedb/issues
- Download: https://pypi.org/project/treedb/#files


Quickstart
----------

Install ``treedb`` (and dependencies):

.. code:: bash

    $ pip install treedb

Clone the Glottolog `master repo`_ :

.. code:: bash

    $ git clone https://github.com/glottolog/glottolog.git

Note: ``treedb`` expects to find it under ``./glottolog/`` by default (i.e. in
the current directory), use ``treedb.set_root()`` to point it to a different
path.

Load ``./glottolog/languoids/tree/**/md.ini`` into an in-memory ``sqlite3`` database.
Write the denormalized example query into ``treedb.query.csv``:

.. code:: bash

    $ python -c "import treedb; treedb.load(); treedb.write_csv()"


Usage from Python
------------------

Start a Python shell:

.. code:: bash

    $ python

Import the package:

.. code:: python

    >>> import treedb

Use ``treedb.iterlanguoids()`` to iterate over languoids as (<path>, ``dict``) pairs:

.. code:: python

    >>> next(treedb.iterlanguoids())
    (('abin1243',), {'id': 'abin1243', 'parent_id': None, 'level': 'language', ...

Note: This is a low-level interface, which does not require loading.

Load the database into ``treedb.sqlite3`` (and set the default ``engine``):

.. code:: python

    >>> treedb.load('treedb.sqlite3')
    ...
    <treedb.proxies.SqliteEngineProxy filename='treedb.sqlite3' ...>

Run consistency checks:

.. code:: python

    >>> treedb.check()
    ...
    True

Export into a ZIP file containing one CSV file per database table:

.. code:: python

    >>> treedb.export()
    ...Path('treedb.zip')

Execute the example query and write it into a CSV file with one row per languoid:

.. code:: python

    >>> treedb.write_csv()
    ...Path('treedb.query.csv')

Rebuild the database (e.g. after an update):

.. code:: python

    >>> treedb.load(rebuild=True)
    ...
    <treedb.proxies.SqliteEngineProxy filename='treedb.sqlite3' ...>

Execute a simple query with ``sqlalchemy`` core and write it to a CSV file:

.. code:: python

    >>> treedb.write_csv(treedb.select([treedb.Languoid]), filename='languoids.csv')
    ...Path('languoids.csv')

Get one row from the ``languoid`` table via `sqlalchemy` core:

.. code:: python

    >>> treedb.select([treedb.Languoid]).execute().first()
    ('abin1243', 'language', 'Abinomn', None, 'bsa', 'bsa', -2.92281, 138.891)

Get one ``Languoid`` model instance via ``sqlalchemy`` orm:

.. code:: python

    >>> session = treedb.Session()
    >>> session.query(treedb.Languoid).first()
    <Languoid id='abin1243' level='language' name='Abinomn' hid='bsa' iso639_3='bsa'>
    >>> session.close()


See also
--------

- pyglottolog_ |--| official Python API to access https://github.com/glottolog/glottolog


License
-------

This tool is distributed under the `MIT license`_.


.. _Glottolog: https://glottolog.org/
.. _master repo: https://github.com/glottolog/glottolog
.. _languoids/tree: https://github.com/glottolog/glottolog/tree/master/languoids/tree
.. _SQLite: https://sqlite.org
.. _languoid: https://glottolog.org/meta/glossary#Languoid
.. _example: https://github.com/glottolog/treedb/blob/36c7cdcdd017e7aa4386ef085ee84fb3036c01ca/treedb/checks.py#L154-L169
.. _pyglottolog: https://github.com/glottolog/pyglottolog
.. _references: https://github.com/glottolog/glottolog/tree/master/references
.. _SQL query: https://github.com/glottolog/treedb/blob/master/treedb/queries.py
.. _most widely used: https://www.sqlite.org/mostdeployed.html
.. _DBeaver: https://dbeaver.io/
.. _sqlite3 cli: https://sqlite.org/cli.html
.. _SQLAlchemy: https://www.sqlalchemy.org
.. _models: https://github.com/glottolog/treedb/blob/master/treedb/models.py
.. _SQLAlchemy Core: https://docs.sqlalchemy.org/en/latest/core/
.. _ORM: https://docs.sqlalchemy.org/en/latest/orm/
.. _venv: https://docs.python.org/3/library/venv.html

.. _MIT license: https://opensource.org/licenses/MIT


.. |--| unicode:: U+2013


.. |PyPI version| image:: https://img.shields.io/pypi/v/treedb.svg
    :target: https://pypi.org/project/treedb/
    :alt: Latest PyPI Version
.. |License| image:: https://img.shields.io/pypi/l/treedb.svg
    :target: https://pypi.org/project/treedb/
    :alt: License
.. |Supported Python| image:: https://img.shields.io/pypi/pyversions/treedb.svg
    :target: https://pypi.org/project/treedb/
    :alt: Supported Python Versions
.. |Format| image:: https://img.shields.io/pypi/format/treedb.svg
    :target: https://pypi.org/project/treedb/
    :alt: Format

.. |Travis| image:: https://travis-ci.org/glottolog/treedb.svg?branch=master
    :target: https://travis-ci.org/glottolog/treedb
    :alt: Travis
.. |Codecov| image:: https://codecov.io/gh/glottolog/treedb/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/glottolog/treedb
    :alt: Codecov