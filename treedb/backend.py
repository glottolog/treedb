# backend.py - sqlite3 database engine

import contextlib
import datetime
import functools
import gzip
import logging
import re
import sqlite3
import warnings
import zipfile

import csv23

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.compiler

try:
    import sqlparse
except ImportError:  # pragma: no cover
    sqlparse = None

from . import (tools as _tools,
               proxies as _proxies)

from . import ENGINE

__all__ = ['print_query_sql', 'get_query_sql', 'expression_compile',
           'set_engine',
           'connect', 'scalar', 'iterrows',
           'registry', 'print_schema',
           'Dataset', 'Producer',
           'backup', 'dump_sql', 'export']


log = logging.getLogger(__name__)


def print_query_sql(query=None, *, literal_binds=True, pretty=True, flush=True):
    """Print the literal SQL for the given query."""
    sql = get_query_sql(query, literal_binds=literal_binds, pretty=pretty)
    print(sql, flush=flush)


def get_query_sql(query=None, *, literal_binds=True, pretty=False):
    """Return the literal SQL for the given query."""
    if query is None:
        from . import queries

        query = queries.get_query()

    compiled = expression_compile(query, literal_binds=literal_binds)
    result = compiled.string

    if pretty and sqlparse is not None:
        result = sqlparse.format(result, reindent=True)
    return result


def expression_compile(expression, *, literal_binds=True):
    """Return literal compiled expression."""
    return expression.compile(compile_kwargs={'literal_binds': literal_binds})


def set_engine(filename, *, resolve=False, require=False, title=None):
    """Return new sqlite3 engine and set it as default engine for treedb."""
    log.info('set_engine: %r', filename)

    if isinstance(filename, sa.engine.Engine):
        engine = filename
        if isinstance(filename, _proxies.EngineProxy):
            engine = engine.engine

        ENGINE.engine = engine
        return ENGINE

    if filename is None:
        if title is not None:
            ENGINE._memory_path = _tools.path_from_filename(f'{title}-memory',
                                                            expanduser=False)
    else:
        filename = _tools.path_from_filename(filename)
        if resolve:
            filename = filename.resolve(strict=False)

        if require and not filename.exists():
            log.error('required engine file not found: %r', filename)
            raise RuntimeError(f'engine file does not exist: {filename!r}')

    ENGINE.file = filename
    log.info('sqlite version: %s', ENGINE.dialect.dbapi.sqlite_version)
    log.info('sqlalchemy version: %s', sa.__version__)
    log.info('csv23 version: %s', csv23.__version__)
    if sqlparse is not None:
        log.info('sqlparse version: %s', sqlparse.__version__)
    return ENGINE


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks."""
    if not isinstance(dbapi_conn, sqlite3.Connection):
        return

    log.debug('connect sqlalchemy.engine.Engine: enable foreign keys')

    with contextlib.closing(dbapi_conn.cursor()) as cursor:
        cursor.execute('PRAGMA foreign_keys = ON')

    log.debug('dbapi_conn: %r', dbapi_conn)


def connect(engine_or_conn=ENGINE):
    if isinstance(engine_or_conn, sa.engine.base.Connection):
        log.debug('nested connect (no-op): %r', engine_or_conn)
        return _tools.nullcontext(engine_or_conn)

    log.debug('engine connect: %r', engine_or_conn)
    conn = engine_or_conn.connect()
    log.debug('conn: %r', conn)
    return conn


def scalar(statement, *args, bind=ENGINE, **kwargs):
    with connect(bind) as conn:
        return conn.scalar(statement, *args, **kwargs)


def iterrows(query, *, mappings=False, bind=ENGINE):
    with connect(bind) as conn:
        result = conn.execute(query)

        if mappings:
            result = result.mappings()

        yield from result


@sa.ext.compiler.compiles(sa.schema.CreateTable)
def compile(element, compiler, **kwargs):
    """Append sqlite3 WITHOUT_ROWID to CREATE_TABLE if configured.

    From https://gist.github.com/chaoflow/3a6dc9d42a90c38870b8d4033b58a4d1
    """
    text = compiler.visit_create_table(element, **kwargs)
    if element.element.info.get('without_rowid'):
        text = text.rstrip() + ' WITHOUT ROWID'
    return text


registry = sa.orm.registry()


def print_schema(metadata=registry.metadata, *, engine=ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql):
        print(sql.compile(dialect=engine.dialect))

    mock_engine = sa.create_mock_engine(engine.url, executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)


@registry.mapped
class Dataset:
    """Git commit loaded into the database."""

    __tablename__ = '__dataset__'

    id = sa.Column(sa.Integer, sa.CheckConstraint('id = 1'), primary_key=True)

    title = sa.Column(sa.Text, sa.CheckConstraint("title != ''"), nullable=False)

    git_commit = sa.Column(sa.String(40), sa.CheckConstraint('length(git_commit) = 40'), nullable=False, unique=True)
    git_describe = sa.Column(sa.Text, sa.CheckConstraint("git_describe != ''"), nullable=False, unique=True)
    clean = sa.Column(sa.Boolean(create_constraint=True), nullable=False)

    exclude_raw = sa.Column(sa.Boolean(create_constraint=True), nullable=False)

    @classmethod
    def get_dataset(cls, *, bind, strict, fallback=None):
        table = cls.__tablename__
        log.debug('read %r from %r', table, bind)

        try:
            result, = iterrows(sa.select(cls), mappings=True, bind=bind)
        except sa.exc.OperationalError as e:
            if 'no such table' in e.orig.args[0]:
                pass
            else:
                log.exception('error selecting %r', table)
                if strict:
                    raise RuntimeError('failed to select %r from %r', table, bind) from e
            return fallback
        except ValueError as e:
            log.exception('error selecting %r', table)
            if 'not enough values to unpack' in e.args[0] and not strict:
                return fallback
            raise RuntimeError('failed to select %r from %r', table, bind) from e
        except Exception as e:  # pragma: no cover
            log.exception('error selecting %r', table)
            raise RuntimeError('failed to select %r from %r', table, bind) from e
        else:
            return result

    @classmethod
    def log_dataset(cls, params):
        name = cls.__tablename__
        log.info('git describe %(git_describe)r clean: %(clean)r', params)
        if not params['clean']:
            warnings.warn(f'{name} not clean')  # pragma: no cover
        log.debug('%s.title: %r', name, params['title'])
        log.info('%s.git_commit: %r', name, params['git_commit'])


@registry.mapped
class Producer:
    """Name and version of the package that created a __dataset__."""

    __tablename__ = '__producer__'

    id = sa.Column(sa.Integer, sa.CheckConstraint('id = 1'), primary_key=True)

    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"),
                     unique=True, nullable=False)

    version = sa.Column(sa.Text, sa.CheckConstraint("version != ''"),
                        nullable=False)

    @classmethod
    def get_producer(cls, *, bind):
        result, = iterrows(sa.select(cls), mappings=True, bind=bind)
        return result

    @classmethod
    def log_producer(cls, params):
        name = cls.__tablename__
        log.info('%s.name: %s', name, params['name'])
        log.info('%s.version: %s', name, params['version'])


def backup(filename=None, *, pages=0, as_new_engine=False, engine=ENGINE):
    """Write the database into another .sqlite3 file and return its engine."""
    log.info('backup database')
    log.info('source: %r', engine)

    url = 'sqlite://'
    if filename is not None:
        path = _tools.path_from_filename(filename)
        if path.exists():
            if engine.file is not None and path.samefile(engine.file):
                raise ValueError(f'backup destination {path!r} same file as'
                                 f' source {engine.file!r}')
            warnings.warn(f'delete present file: {path!r}')
            path.unlink()
        url += f'/{path}'

    log.info('destination: %r', url)
    result = sa.create_engine(url, future=engine.future)

    def progress(status, remaining, total):
        log.info('%d of %d pages copied', total - remaining, total)

    with contextlib.closing(engine.raw_connection()) as source,\
         contextlib.closing(result.raw_connection()) as dest:
        log.debug('sqlite3.backup(%r)', dest.connection)

        dest.execute('PRAGMA synchronous = OFF')
        dest.execute('PRAGMA journal_mode = MEMORY')

        with dest.connection as dbapi_conn:
            source.backup(dbapi_conn, pages=pages, progress=progress)

    log.info('database backup complete')
    if as_new_engine:
        set_engine(result)
    return result


def dump_sql(filename=None, *, progress_after=100_000,
             encoding=_tools.ENCODING, engine=ENGINE):
    """Dump the engine database into a plain-text SQL file."""
    if filename is None:
        filename = engine.file_with_suffix('.sql.gz').name
    path = _tools.path_from_filename(filename)
    log.info('dump sql to %r', path)

    if path.exists():
        warnings.warn(f'delete present file: {path!r}')
        path.unlink()

    if path.suffix == '.gz':
        open_path = functools.partial(gzip.open, path)
    else:
        open_path = path.open

    n = 0
    with contextlib.closing(engine.raw_connection()) as dbapi_conn,\
         open_path('wt', encoding=encoding) as f:
        for n, line in enumerate(dbapi_conn.iterdump(), 1):
            print(line, file=f)
            if not (n % progress_after):
                log.info('%s lines written', f'{n:_d}')

    log.info('%s lines total', f'{n:_d}')
    return path


def export(filename=None, *, exclude_raw=False, metadata=registry.metadata,
           dialect=csv23.DIALECT, encoding=csv23.ENCODING, engine=ENGINE):
    """Write all tables to <tablename>.csv in <databasename>.zip."""
    log.info('export database')
    log.debug('engine: %r', engine)

    if filename is None:
        filename = engine.file_with_suffix('.zip').name

    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'delete present file: {path!r}')
        path.unlink()

    filename = str(path)

    sorted_tables = sorted(metadata.sorted_tables, key=lambda t: t.name)

    skip = {'_file', '_option', '_value'} if exclude_raw else {}

    log.info('write %r', filename)
    with connect(engine) as conn,\
         zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in sorted_tables:
            if table.name in skip:
                log.debug('skip table %r', table.name)
                continue

            log.info('export table %r', table.name)
            rows = conn.execute(table.select())
            header = list(rows.keys())

            date_time = datetime.datetime.now().timetuple()[:6]
            info = zipfile.ZipInfo(f'{table.name}.csv', date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED

            with z.open(info, 'w') as f:
                csv23.write_csv(f, rows, header=header,
                                dialect=dialect, encoding=encoding)

    log.info('database exported')
    return _tools.path_from_filename(filename)
