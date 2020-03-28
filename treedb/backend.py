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
               proxies as _proxies)

from . import ROOT, ENGINE

__all__ = ['set_engine',
           'Model', 'print_schema',
           'Dataset',
           'Session',
           'load',
           'dump_sql', 'export', 'backup',
           'print_table_sql', 'print_query_sql', 'select_stats']


log = logging.getLogger(__name__)


def set_engine(filename, *, resolve=False, title=None):
    """Return new sqlite3 engine and set it as default engine for treedb."""
    log.info('set_engine')
    if isinstance(filename, sa.engine.Engine):
        engine = filename
        if isinstance(filename, _proxies.EngineProxy):
            engine = engine.engine
        log.debug('engine: %r', engine)
        ENGINE.engine = engine
        return ENGINE

    log.debug('filename: %r', filename)
    if filename is not None:
        filename = _tools.path_from_filename(filename)
        if resolve:
            filename = filename.resolve(strict=False)

    if filename is None and title is not None:
        ENGINE._memory_path = _tools.path_from_filename(f'{title}-memory',
                                                        expanduser=False)

    ENGINE.file = filename
    return ENGINE


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks, enable REGEXP operator."""
    log.debug('engine connect (enable foreign keys and regexp operator)')

    with contextlib.closing(dbapi_conn.cursor()) as cursor:
        cursor.execute('PRAGMA foreign_keys = ON')

    dbapi_conn.create_function('regexp', 2, _regexp)


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


Session = sa.orm.sessionmaker(bind=ENGINE)


def load(filename=ENGINE, repo_root=None, *,
         treepath=_files.TREE_IN_ROOT,
         require=False, rebuild=False,
         exclude_raw=False, from_raw=None, force_delete=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    log.info('load database')

    if hasattr(filename, 'execute'):
        engine = filename
    else:
        engine = set_engine(filename)

    if repo_root is not None:
        root = _files.set_root(repo_root, treepath=treepath)
    else:
        root = ROOT
    if not root.exists():
        log.error('root does not exist')
        raise RuntimeError(f'tree root not found: {root!r}')

    if require and not engine.file_exists():
        log.error('required load file not found: %r', engine.file)
        raise RuntimeError(f'engine file does not exist: {engine.file!r}')

    if exclude_raw and from_raw:
        log.error('incompatible exclude_raw=%r'
                  ' and from_raw=%r', exclude_raw, from_raw)
        raise RuntimeError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw

    assert engine.url.drivername == 'sqlite'
    if engine.file is None:
        log.debug('dispose engine %r', engine)
        engine.dispose()
        log.warning('connected to a transient in-memory database')
    elif engine.file.exists():
        log.debug('read %r from %r', Dataset.__tablename__, engine.file)

        try:
            ds, = sa.select([Dataset], bind=engine).execute()
        except Exception:
            ds = None
            log.exception('error reading %r', Dataset.__tablename__)
            if not force_delete:
                raise

            warnings.warn(f'force delete {engine.file!r}')
            rebuild = True
        else:
            if ds.exclude_raw != bool(exclude_raw):
                log.info('rebuild needed from exclude_raw mismatch')

        if rebuild or ds.exclude_raw != bool(exclude_raw):
            log.info('rebuild database')
            log.debug('dispose engine %r', engine)
            engine.dispose()

            warnings.warn(f'delete present file: {engine.file!r}')
            engine.file.unlink()
        else:
            log.info('use present %r', engine.file)
            log_dataset(dict(ds))
            return engine

    @contextlib.contextmanager
    def begin(bind=engine):
        log.debug('begin transaction on %r', bind)
        with bind.begin() as conn:
            conn.execute('PRAGMA synchronous = OFF')
            conn.execute('PRAGMA journal_mode = MEMORY')
            conn = conn.execution_options(compiled_cache={})

            log.debug('conn: %r', conn)
            yield conn
        log.debug('end transaction on %r', bind)

    # import here to register models for create_all()
    if not exclude_raw:
        log.debug('import module raw')
        from . import raw

    log.debug('import module models_load')
    from . import models_load

    application_id = sum(ord(c) for c in Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    log.debug('start load timer')
    start = time.time()

    log.info('create tables')
    with begin() as conn:
        log.debug('set application_id = %r', application_id)
        conn.execute(f'PRAGMA application_id = {application_id:d}')

        log.debug('run create_all')
        Model.metadata.create_all(bind=conn)

    log.info('record git commit')
    log.debug('cwd: %r', root)
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

    log.info('write %r', Dataset.__tablename__)
    with begin() as conn:
        log.debug('dataset: %r', dataset)
        sa.insert(Dataset, bind=conn).execute(dataset)

    walltime = datetime.timedelta(seconds=time.time() - start)
    log.debug('load timer stopped')

    log.info('database loaded')
    log_dataset(dataset)
    print(walltime)
    return engine


def log_dataset(params, *, name=Dataset.__tablename__):
    log.info('git describe %(git_describe)r clean: %(clean)r', params)
    if not params['clean']:
        warnings.warn(f'{name} not clean')
    log.debug('%r.title: %r', name, params['title'])
    log.debug('%r.git_commit: %r', name, params['git_commit'])


def dump_sql(filename=None, *, encoding=_tools.ENCODING, engine=ENGINE):
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
            if not (n % 100_000):
                log.debug('%d lines written', n)

    log.info('%d lines total', n)
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
        log.debug('%d of %d pages copied', total - remaining, total)

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
    if query is None:
        from . import queries
        query = queries.get_query()

    s = query.compile(compile_kwargs={'literal_binds': literal_binds})
    print(s)


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
