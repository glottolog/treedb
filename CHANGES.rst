Changelog
=========


Version 2.4.5 (in development)
------------------------------




Version 2.4.4
-------------

Fix ``queries.get_json_query()`` SQLite 3.39 compatibility.


Version 2.4.3
-------------

Add test checksums for Glottolog ``v4.6``.


Version 2.4.2
-------------

Drop Python 3.6 support.


Version 2.4.1
-------------

Disable ``treedb.checks.bookkeeping_no_children()``
for upstream change in Glottolog ``v4.5``.

Add test checksums for Glottolog ``v4.5``.

Add ``lint-code.py``.

Tag Python 3.10 support.


Version 2.4
-----------

Rename table ``endangerment_source`` to ``endangerment_source``.

Change ``iterfiles()`` to yield ``path_tuple, DirEntry, ConfigParser`` instead
of ``path_tuple, ConfigParser, DirEntry``.

Include ``version`` from ``config/publication.ini`` in table ``__dataset__``.

Enforce unique ``pseudofaimily`` with ``bookkeeping`` by using ``NULL`` instead
of ``FALSE`` for non-bookkeeping rows.

Add ``write_files(source='raw_lines')`` as shortcut for ``raw.write_files()``.

Fix ``write_files(source='raw')``.

Implement ``records.pipe(dump=True, convert_lines=True)``.

Reorganize ``queries.py`` for better code re-use, refactor example query
using more explicit ``JOIN`` syntax and more uniform aliases.

Improve internal package structure with ``languoids`` submodule. Move
``ConfigParser`` to ``_tools.py``. Centralize repo git commands in
`glottolog.py``.

Add ``run-tests.py`` command-line flags: ``--run-writes``, ``--skip-pandas``,
``--skip-sqlparse``, ``--file-engine-tag``, ``--no-sqlalchemy-warn-20`` and
use custom markers and ``pytestconfig`` for test setup.

Increase test coverage. Move shared helpers to ``tests/helpers.py``. Improve
test ids and summary output. Include Glottolog tag/branch in output file names.

Add test checksum for Glottolog ``v4.4``.
Drop Glottolog ``v4.2`` test hashes (in favour of ``v4.2.1``).
Add xfail for Glottolog ``master`` branch checksum equivalence.
Promote skips to xfails.

Migrate CI from Travis to GitHub Actions.


Version 2.3
-----------

Add ``Config`` model  and ``_config?`` table with ``glottolog/config/*.ini`` content.

Load languoid levels, macroareas, and endangerment status from ``Config``,
compare pseudofamily ``Config`` with treedb constants.

Add test checksums for Glottolog ``v4.4``.

Improve test coverage.


Version 2.2
-----------

Add ``pd.read_json_lines()``.

Add ``dry_run`` parameter to ``write_files()`` functions.

Improve test environment and increase test coverage.

Improve usage example ``try-treedb.py``.


Version 2.1.2
-------------

Insert languoids in Glottocode order again when loading from raw.


Version 2.1.1
-------------

Fix ``write_files()``.


Version 2.1
-----------

Bump ``pandas`` optional dependency to ``>=1``.

Add ``pd_read_languoids()`` reading json lines.

Add ``limit`` and ``offset`` parameters to ``iterlanguoids()``, ``checksum()``,
``write_languoids()``, and ``pd_read_languoids()``.

Increase test data coverage.

Clean up namespaces, add more type annotations.


Version 2.0
-----------

Add ``source`` parameter to ``iterlanguoids()``, ``checksum()``, and ``write_json_lines()``
to use ``'files'``, ``'raw'``, or ``'tables'`` as data source. 

Changed checksums from ``path_json:id:...`` to ``path_languoid:path:...`` over json lines.

Rename ``treedb.write_json_lines()`` to `` treedb.write_languoids()`` making it equal to checksum.

Rename ``compare_with_files()`` to ``compare_languoids()``.

Rename ``get_json_query()`` to ``get_languoids_query()``.

Rename ``get_query()`` to ``get_example_query()``.


Version 1.5
-----------

Rename ``treedb.export()`` to ``treedb.csv_zipfile()``.

Update SQLAlchemy to 1.4 with FUTURE=True:
- remove bound select (use sqlalchemy select with scalar,
  iterrows, print_rows, write_csv, etc., or with  connect)
- raw now records information about flag definitions
- improved raw.print_stats() order

Add ``print_versions()`` and ``print_dataset()``.

Simplify count queries in checks.

Improve package structure.

Improve block-style code-formatting.

Improve test environment: add --skip-slow and --log-sql


Version 1.4.1
-------------

Add workaround for https://bugs.python.org/issue18199.


Version 1.4
-----------

Add support for new minimal countries format in glottolog md.ini files
(see https://github.com/glottolog/glottolog/pull/636). The previous full format
continues to be supported for reading older versions of the repository.


Version 1.3.5
-------------

Add Python 3.9 to test environments and tag support.


Version 1.3.4
-------------

Pin sqlalchemy dependency to version ``1.3.*``.

Add explicit ``.alias()`` or ``.as_scalar()`` to implicit subqueries.

Decrease progress logging verbosity.

Add test checksums for Glottolog ``v4.3-treedb-fixes``.

Improve test reporting, update test environment.


Version 1.3.3
-------------

Restrict foreign key pragma and regexp operator to sqlite3 connections.


Version 1.3.2
-------------

Format timespan years with 4 digits.

Refactor recursive tree queries to improve code sharing.

Simplify stats query and ``iterdescendants()`` query.

Stats.ipynb: fix n_descendants query, add count to min/max boxes, plot more
frequency distributions.


Version 1.3.1
-------------

Fix default root (change from . to ./glottolog/ as documented).

Fix ``treedb.write_json_lines()`` under Python 3.6 when passed a file object.

Extend showcase notebook and test coverage.


Version 1.3
-----------

Add ``treedb.write_json_lines()``.

Use ``lang=None`` outside of the database (checksum change).


Version 1.2
-----------

Normalize providers and sites into lookup tables.

Fix exception when config file is not found.

Improve logging: log sqlite3.Connection to identify in-memory databases,
debug log package location.

Improve tests.


Version 1.1
-----------

Fix some aggregation orders in ``get_json_query()`` that depended on the
insertion order.

Fix ```get_query()`` link markup. Use the same aggregation order as
``get_json_query``.

Fixed ``treedb.iterdescendants()`` to include roots with no descencants.

Improve ``treedb.print_languoid_stats()`` performance and the query used for
the ``stats`` view.

Reduce file size adding WITHOUT ROWID to tables with non-integer or composite
primary keys.

Stabilize ``treedb.print_query_sql()`` notebook output with ``flush=True``.

Change __dataset__ and __producer__ primary key from BOOLEAN to INTEGER.

Use ``sqlite.sqlite_version`` instead of querying the engine.


Version 1.0
-----------

Build with Glottolog ``v4.2.1`` per default.

Improve ``treedb.print_schema()`` output for views.

Improve tests and logging.


Version 0.11
------------

Add support for the new optional core `timespan` field.

Add new test flags: --glottolog-repo-root and --force-rebuild.

Extend tests and integrate with Travis and Codevov.


Version 0.10
------------

Insert languoids in ``id`` order if possible.

Gzip dump-like csv files per default (bump csv23 to 0.3+).

Change default name of ``treedb.write_csv`` to ``treedb.query.csv```.

Register ``pandas`` as optional dependency.

Fix xenial compat. Fix Python 3.6 compat.

Fix re-load with ``exclude_raw``.

Improve logging.

Increase test coverage. Log ``sqlite_version()``.


Version 0.9
-----------

Add ``treedb.checkout_or_clone()``.

Add ``treedb.print_query_sql(pretty=True)`` formatting with ``sqlparse`` if
importable (``pip install treedb[pretty]`` to include it).

Improve query readability by adding unique labels.

Move recurse condition for ``Languoid.tree()`` from whereclause to join.

Add tests using ``pytest``.


Version 0.8.2
-------------

Add ``example`` view with ``treedb.get_query()``.

Reorganized `treedb.load()` to better support repeated changes to
`exclude_views`.


Version 0.8.1
-------------

Add ``roots`` (top-level languoids) count to ``treedb.print_languoid_stats()``.

Gzip-compress `treedb.dump_sql()` by default.

Fix reference to old license.

Add ``raw=False`` to ``treedb.write_json_query_csv()``.

Update documentation.


Version 0.8
-----------

Add ``stats`` and ``path_json`` SQL views.

Extend formatting of ``treedb.print_languoid_stats()```and warn in case of
inconsistencies.

Fix ``Languoid.tree(include_self=False)``.

Add names to query aliases for better SQL output.


Version 0.7.1
-------------

Add ``treedb.print_languoid_stats()`` (reproducing
https://glottolog.org/glottolog/glottologinformation).

Add ``treedb.write_json_query_csv()``. To support this,
``treedb.get_json_query()`` now yields pairs of path and languoid json
(instead of json of a two-item array with a path_part array as first element).


Version 0.7
-----------

Add ``treedb.configure()`` trying to read the Glottolog ``repo_root`` from
``treedb.ini`` in the current working directory.

Add ``logging`` config to the development environment as ``treedb.ini``
(write ``treedb.log``).

Added handling of present databases when loading from transient in-memory
database.

Add ``__producer__`` table recording the ``treedb`` package version used.


Version 0.6
-----------

Add ``treedb.checksum()``.


Version 0.5.1
-------------

Add ``replace`` kwarg to ``treedb.write_files()`` and
``treedb.raw.write_files()``.


Version 0.5
-----------

Add ``treeb.write_files()``.

Replace ``treedb.compare_with_raw(root, bind)`` with
``treedb.compare_with_files(bind, root)``.


Version 0.4
-----------

Add ``treedb.print_query_sql()`` for printing standalone SQL with literal
binds that can be pasted into query tools.

Normalize bibitem references (change database structure).


Version 0.3.1
-------------

Add missing ``os.path.expanduser()`` to ``treedb.export()``.

``treedb.pd_read_sql()`` now uses the default query when called without
argument.

Add ``csv23`` as dependency (factored out stream handling).


Version 0.3
-----------

Rename ``treedb.create_engine()`` to ``treedb.set_engine()``
(backwards incompatible).

Improve ``treedb.backup()`` implementation.

Add helper functions and shortcuts.

Refactor ``subprocess`` usage.

Improve logging.


Version 0.2.2
-------------

Add ``treedb.backup()`` (requires Python 3.7+).

Add ``exclude_raw=False`` to ``treedb.export()``.


Version 0.2.1
-------------

Fix ``treedb.write_csv()`` endangerment_source column output.

Use ``os.path.expanduser()`` on filename arguments.

Fix setup.py old license classifier.


Version 0.2
-----------

Drop Python 2 and 3.5 support.

Parse endangerment source references into individual fields.

Switch license to MIT license.


Version 0.1.6
-------------

Make endangerment sources open-ended.


Version 0.1.5
-------------

Update endangerment sources.

Add PyPI ``project_urls`` to setup.py.


Version 0.1.4
-------------

Represent countries as dicts instead of tuples in ``treedb.iterlanguoids()``.

Update endangerment sources.


Version 0.1.3
-------------

Update for new altname providers and endangerment sources in Glottolog ``v4.1``.


Version 0.1.2
-------------

Use ``expanduser()``in ``treedb.create_engine()`` and ``treedb.set_root()``.


Version 0.1.1
-------------

Allow to specify glottolog repository location via ``TREEDB_REPO`` environment
variable (alternative to ``treedb.set_root()``).

Use current working as default Glottolog repository location if ``treedb`` is
imported as plain installed package instead of a git checkout.


Version 0.1
-----------

Initial release.
