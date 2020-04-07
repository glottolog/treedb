# backend.py - sqlite3 database engine

import contextlib
import datetime
import functools
import logging
import re
import time
import warnings
import zipfile

import csv23

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from . import (tools as _tools,
               files as _files,
               proxies as _proxies,
               backend_views as _views)

from . import ROOT, ENGINE

__all__ = ['set_engine',
           'Model', 'print_schema',
           'Dataset', 'Producer',
           'Session',
           'load',
           'dump_sql', 'export', 'backup',
           'print_table_sql',
           'print_query_sql', 'get_query_sql', 'expression_compile',
           'view', 'create_view',
           'select_stats']


log = logging.getLogger(__name__)


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
    if value is None:
        return None
    return re.search(pattern, value) is not None


Model = sa.ext.declarative.declarative_base()


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


def load(filename=ENGINE, repo_root=None, *,
         treepath=_files.TREE_IN_ROOT,
         require=False, rebuild=False,
         exclude_raw=False, from_raw=None,
         exclude_views=False,
         force_rebuild=False,
         metadata=Model.metadata):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    if repo_root is not None:
        root = _files.set_root(repo_root, treepath=treepath)
    else:
        root = ROOT

    log.info('load database from %r', root)
    if not root.exists():
        log.error('root does not exist')
        raise RuntimeError(f'tree root not found: {root!r}')

    if exclude_raw and from_raw:
        log.error('incompatible exclude_raw=%r and from_raw=%r', exclude_raw, from_raw)
        raise ValueError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw

    if hasattr(filename, 'execute'):
        engine = filename
    else:
        engine = set_engine(filename, require=require)

    if engine.file is None:
        log.warning('connected to a transient in-memory database')
        ds = Dataset.get_dataset(bind=engine, strict=True)

        if ds is not None and not rebuild:
            log.info('use present %r', engine.url)
            Dataset.log_dataset(dict(ds))
            producer = Producer.get_producer(bind=engine)
            Producer.log_producer(dict(producer))
            return engine

        if rebuild or (ds is None and force_rebuild):
            log.info('rebuild database')
            engine.dispose()

    elif engine.file_exists():
        ds = Dataset.get_dataset(bind=engine, strict=not force_rebuild)
        if ds is None:
            warnings.warn(f'force delete {engine.file!r}')
            rebuild = True
        elif ds.exclude_raw != bool(exclude_raw):
            log.info('rebuild needed from exclude_raw mismatch')
            rebuild = True

        if not rebuild:
            log.info('use present %r', engine)
            Dataset.log_dataset(dict(ds))
            pdc = Producer.get_producer(bind=engine)
            Producer.log_producer(dict(pdc))
            return engine

        log.info('rebuild database')
        engine.dispose()

        warnings.warn(f'delete present file: {engine.file!r}')
        engine.file.unlink()

    @contextlib.contextmanager
    def begin(bind=engine):
        log.debug('begin transaction on %r', bind)
        with bind.begin() as conn:
            conn.execute('PRAGMA synchronous = OFF')
            conn.execute('PRAGMA journal_mode = MEMORY')
            conn = conn.execution_options(compiled_cache={})

            yield conn

        log.debug('end transaction on %r', bind)

    # import here to register models for create_all()
    if not exclude_raw:
        log.debug('import module %s.raw', __package__)
        from . import raw

    if not exclude_views:
        log.debug('import module %s.views', __package__)
        from . import views

    log.debug('import module %s.models_load', __package__)
    from . import models_load

    application_id = sum(ord(c) for c in Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    log.debug('start load timer')
    start = time.time()

    log.info('create %d tables from %r', len(metadata.tables), metadata)
    with begin() as conn:
        log.debug('set application_id = %r', application_id)
        conn.execute(f'PRAGMA application_id = {application_id:d}')

        log.debug('run create_all')
        metadata.create_all(bind=conn)

    log.info('record git commit in %r', root)
    run = functools.partial(_tools.run, cwd=str(root),
                            capture_output=True, unpack=True)

    try:
        dataset = {'title': 'Glottolog treedb',
                   'git_commit': run(['git', 'rev-parse', 'HEAD']),
                   'git_describe': run(['git', 'describe', '--tags', '--always']),
                   # neither changes in index nor untracked files
                   'clean': not run(['git', 'status', '--porcelain']),
                   'exclude_raw': exclude_raw}
    except Exception:
        log.exception('error running git command in %r', str(root))
        raise

    from . import __version__

    producer = {'name': __package__, 'version': __version__}
    log.info('write %r: %r', Producer.__tablename__, producer)
    with begin() as conn:
        sa.insert(Producer, bind=conn).execute(producer)

    if not exclude_raw:
        log.info('load raw')
        with begin() as conn:
            log.debug('root: %r', root)
            raw.load(root, conn)

    if not (from_raw or exclude_raw):
        warnings.warn('2 tree reads required (use compare_with_files() to verify)')

    log.debug('import module languoids')
    from . import languoids

    log.info('load languoids')
    with begin() as conn:
        root_or_bind = conn if from_raw else root
        log.debug('root_or_bind: %r', root_or_bind)

        pairs = languoids.iterlanguoids(root_or_bind, from_raw=from_raw)
        models_load.load(pairs, conn)

    log.info('write %r: %r', Dataset.__tablename__, dataset['title'])
    with begin() as conn:
        log.debug('dataset: %r', dataset)
        sa.insert(Dataset, bind=conn).execute(dataset)

    walltime = datetime.timedelta(seconds=time.time() - start)
    log.debug('load timer stopped')

    log.info('database loaded')
    Dataset.log_dataset(dataset)
    print(walltime)
    return engine


def dump_sql(filename=None, *, progress_after=100_000,
             encoding=_tools.ENCODING, engine=ENGINE):
    """Dump the engine database into a plain-text SQL file."""
    if filename is None:
        filename = engine.file_with_suffix('.sql').name
    path = _tools.path_from_filename(filename)
    log.info('dump sql to %r', path)

    if path.exists():
        warnings.warn(f'delete present file: {path!r}')
        path.unlink()

    n = 0
    with contextlib.closing(engine.raw_connection()) as dbapi_conn,\
         path.open('wt', encoding=encoding) as f:
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


def print_table_sql(model_or_table, *, include_nrows=True, bind=ENGINE):
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


def print_query_sql(query=None, literal_binds=True):
    print(get_query_sql(query, literal_binds=literal_binds))


def get_query_sql(query=None, literal_binds=True):
    if query is None:
        from . import queries

        query = queries.get_query()

    compiled = expression_compile(query, literal_binds=literal_binds)
    return compiled.string


def expression_compile(expression, literal_binds=True):
    return expression.compile(compile_kwargs={'literal_binds': literal_binds})


def view(name, *, metadata=Model.metadata):
    def decorator(func):
        selectable = func()
        return create_view(name, selectable, metadata=metadata)

    return decorator


def create_view(name, selectable, *, metadata=Model.metadata):
    log.debug('create_view %r on %r', name, metadata)
    return _views.view(name, selectable, metadata=metadata)


sqlite_master = sa.table('sqlite_master', *map(sa.column, ['name',
                                                           'type',
                                                           'sql']))


def select_sql(table_name, *, bind=ENGINE):
    result = sa.select([sqlite_master.c.sql], bind=bind)\
        .where(sqlite_master.c.type == 'table')\
        .where(sqlite_master.c.name == sa.bindparam('table_name'))

    if table_name is not None:
        result = result.params(table_name=table_name)
    return result


def select_stats(*, bind=ENGINE):
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
