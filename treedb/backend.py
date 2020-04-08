# backend.py - sqlite3 database engine

import contextlib
import datetime
import gzip
import io
import logging
import re
import warnings
import zipfile

import csv23

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from . import (tools as _tools,
               proxies as _proxies,
               backend_views as _views)

from . import ENGINE

__all__ = ['print_query_sql', 'get_query_sql', 'expression_compile',
           'set_engine',
           'Model', 'create_view', 'print_schema',
           'Dataset', 'Producer',
           'Session',
           'backup', 'dump_sql', 'export',
           'print_table_sql',
           'select_stats']


log = logging.getLogger(__name__)


def print_query_sql(query=None, literal_binds=True):
    """Print the literal SQL for the given query."""
    print(get_query_sql(query, literal_binds=literal_binds))


def get_query_sql(query=None, literal_binds=True):
    """Return the literal SQL for the given query."""
    if query is None:
        from . import queries

        query = queries.get_query()

    compiled = expression_compile(query, literal_binds=literal_binds)
    return compiled.string


def expression_compile(expression, literal_binds=True):
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
    return ENGINE


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks, enable REGEXP operator."""
    log.debug('engine connect (enable foreign keys and regexp operator)')

    with contextlib.closing(dbapi_conn.cursor()) as cursor:
        cursor.execute('PRAGMA foreign_keys = ON')

    dbapi_conn.create_function('regexp', 2, _regexp)
    log.debug('conn: %r', dbapi_conn)


def _regexp(pattern, value):
    """Implement the REGEXP operator for sqlite3."""
    if value is None:
        return None
    return re.search(pattern, value) is not None


Model = sa.ext.declarative.declarative_base()


def create_view(name, selectable, *, metadata=Model.metadata):
    """Register a CREATE VIEW for the given selectable on metadata."""
    log.debug('create_view %r on %r', name, metadata)
    return _views.view(name, selectable, metadata=metadata)


def print_schema(metadata=Model.metadata, *, engine=ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql):
        print(sql.compile(dialect=engine.dialect))

    mock_engine = sa.create_engine(engine.url, strategy='mock',
                                   executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)


class Dataset(Model):
    """Git commit loaded into the database."""

    __tablename__ = '__dataset__'

    id = sa.Column(sa.Boolean, sa.CheckConstraint('id'),
                   primary_key=True, server_default=sa.true())

    title = sa.Column(sa.Text, sa.CheckConstraint("title != ''"), nullable=False)

    git_commit = sa.Column(sa.String(40), sa.CheckConstraint('length(git_commit) = 40'), nullable=False, unique=True)
    git_describe = sa.Column(sa.Text, sa.CheckConstraint("git_describe != ''"), nullable=False, unique=True)
    clean = sa.Column(sa.Boolean, nullable=False)

    exclude_raw = sa.Column(sa.Boolean, nullable=False)

    @classmethod
    def get_dataset(cls, *, bind, strict, fallback=None):
        table = cls.__tablename__
        log.debug('read %r from %r', table, bind)

        try:
            result, = sa.select([cls], bind=bind).execute()
        except sa.exc.OperationalError as e:
            if 'no such table' in e.orig.args[0]:
                pass
            else:
                log.exception('error selecting %r', table)
                if strict:
                    raise RuntimeError('failed to select %r from %r', table, bind)
            return fallback
        except ValueError as e:
            log.exception('error selecting %r', table)
            if 'not enough values to unpack' in e.args[0] and not strict:
                return fallback
            raise RuntimeError('failed to select %r from %r', table, bind)
        except Exception:
            log.exception('error selecting %r', table)
            raise RuntimeError('failed to select %r from %r', table, bind)
        else:
            return result

    @classmethod
    def log_dataset(cls, params):
        name = cls.__tablename__
        log.info('git describe %(git_describe)r clean: %(clean)r', params)
        if not params['clean']:
            warnings.warn(f'{name} not clean')
        log.debug('%r.title: %r', name, params['title'])
        log.debug('%r.git_commit: %r', name, params['git_commit'])


class Producer(Model):
    """Name and version of the package that created a __dataset__."""

    __tablename__ = '__producer__'

    id = sa.Column(sa.Boolean, sa.CheckConstraint('id'),
                   primary_key=True, server_default=sa.true())

    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"),
                     unique=True, nullable=False)

    version = sa.Column(sa.Text, sa.CheckConstraint("version != ''"),
                        nullable=False)

    @classmethod
    def get_producer(cls, *, bind):
        result, = sa.select([cls], bind=bind).execute()
        return result

    @classmethod
    def log_producer(cls, params):
        name = cls.__tablename__
        log.info('%s.name: %s', name, params['name'])
        log.info('%s.version: %s', name, params['version'])


Session = sa.orm.sessionmaker(bind=ENGINE)


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
    result = sa.create_engine(url)

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
        @contextlib.contextmanager
        def open_path(encoding):
            with gzip.open(path, 'wb') as z,\
                 io.TextIOWrapper(z, encoding=encoding) as f:
                yield f
    else:
        open_path = path.open

    n = 0
    with contextlib.closing(engine.raw_connection()) as dbapi_conn,\
         open_path(encoding=encoding) as f:
        for n, line in enumerate(dbapi_conn.iterdump(), 1):
            print(line, file=f)
            if not (n % progress_after):
                log.info('%s lines written', f'{n:_d}')

    log.info('%s lines total', f'{n:_d}')
    return path


def export(filename=None, *, exclude_raw=False, metadata=Model.metadata,
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
    with engine.connect() as conn,\
         zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in sorted_tables:
            if table.name in skip:
                log.debug('skip table %r', table.name)
                continue

            log.info('export table %r', table.name)
            rows = table.select(bind=conn).execute()
            header = rows.keys()

            date_time = datetime.datetime.now().timetuple()[:6]
            info = zipfile.ZipInfo(f'{table.name}.csv', date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED

            with z.open(info, 'w') as f:
                csv23.write_csv(f, rows, header=header,
                                dialect=dialect, encoding=encoding)

    log.info('database exported')
    return _tools.path_from_filename(filename)


def print_table_sql(model_or_table, *, include_nrows=True, bind=ENGINE):
    """Print CREATE TABLE for the given table and its number of rows."""
    if hasattr(model_or_table, '__tablename__'):
        table_name = model_or_table.__tablename__
        label = model_or_table.__name__.lower()
    elif hasattr(model_or_table, 'name'):
        table_name = label = model_or_table.name
    else:
        table_name = label = model_or_table
        model_or_table = sa.text(model_or_table)

    print(select_sql(table_name, bind=bind).scalar())

    if include_nrows:
        select_nrows = sa.select([
                sa.func.count().label(f'n_{label}s'),
            ], bind=bind).select_from(model_or_table)
        print(select_nrows.scalar())


def select_sql(table_name, *, bind=ENGINE):
    """Select CREATE_TABLE of the given table name from sqlite_master."""
    result = sa.select([sqlite_master.c.sql], bind=bind)\
        .where(sqlite_master.c.type == 'table')\
        .where(sqlite_master.c.name == sa.bindparam('table_name'))

    if table_name is not None:
        result = result.params(table_name=table_name)
    return result


sqlite_master = sa.table('sqlite_master', *map(sa.column, ['name',
                                                           'type',
                                                           'sql']))


def select_stats(*, bind=ENGINE):
    """Select table name and number ofrows for all tables from sqlite_master."""
    table_name = sqlite_master.c.name.label('table_name')

    select_tables = sa.select([table_name], bind=bind)\
        .where(sqlite_master.c.type == 'table')\
        .where(~table_name.like('sqlite_%'))\
        .order_by('table_name')

    def iterselects(tables_result):
        for t, in tables_result:
            table_name = sa.literal(t).label('table_name')
            n_rows = (sa.select([sa.func.count()])
                      .select_from(sa.table(t))
                      .label('n_rows'))
            yield sa.select([table_name, n_rows])

    tables_result = select_tables.execute()
    return sa.union_all(*iterselects(tables_result), bind=bind)
