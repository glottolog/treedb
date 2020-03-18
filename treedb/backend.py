# backend.py - sqlite3 database engine

from __future__ import unicode_literals
from __future__ import print_function

import re
import time
import logging
import zipfile
import datetime
import warnings
import functools
import contextlib

from ._compat import DIALECT, ENCODING

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from . import files as _files
from . import tools as _tools

from . import ENGINE, ROOT

__all__ = [
    'create_engine',
    'Model', 'print_schema',
    'Dataset',
    'Session',
    'load', 'dump_sql', 'export',
]


log = logging.getLogger(__name__)


def create_engine(filename, resolve=False, title=None):
    """Return new sqlite3 engine and set it as default engine for treedb."""
    log.info('create_engine')
    log.debug('filename: %r', filename)

    if filename is not None:
        filename = _tools.path_from_filename(filename).expanduser()
        if resolve:
            filename = filename.resolve(strict=False)

    if filename is None and title is not None:
        ENGINE._memory_path = _tools.path_from_filename('%s-memory' % title)

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


def print_schema(metadata=Model.metadata, engine=ENGINE):
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


def load(filename=ENGINE, repo_root=None, treepath=_files.TREE_IN_ROOT,
         require=False, rebuild=False,
         exclude_raw=False, from_raw=None, force_delete=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    log.info('load database')

    if hasattr(filename, 'execute'):
        engine = filename
    else:
        engine = create_engine(filename)

    if repo_root is not None:
        root = _files.set_root(repo_root, treepath=treepath)
    else:
        root = ROOT
    if not root.exists():
        log.error('root does not exist')
        raise RuntimeError('tree root not found: %r' % root)

    if require and not engine.file_exists():
        log.error('required load file not found: %r', engine.file)
        raise RuntimeError('engine file does not exist: %r' % engine.file)

    if exclude_raw and from_raw:
        log.error('incompatible exclude_raw=%r'
                  ' and from_raw=%r', exclude_raw, from_raw)
        raise RuntimeError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw

    assert engine.url.drivername == 'sqlite'
    if engine.file is not None and engine.file.exists():
        log.debug('read %r from %r', Dataset.__tablename__, engine.file)

        try:
            ds, = sa.select([Dataset], bind=engine).execute()
        except Exception:
            ds = None
            log.exception('error reading %r', Dataset.__tablename__)
            if not force_delete:
                raise

            warnings.warn('force delete %r' % engine.file)
            rebuild = True
        else:
            if ds.exclude_raw != bool(exclude_raw):
                log.info('rebuild needed from exclude_raw mismatch')

        if rebuild or ds.exclude_raw != bool(exclude_raw):
            log.info('rebuild database')
            log.debug('dispose engine %r', engine)
            engine.dispose()

            warnings.warn('delete present file: %r' % engine.file)
            engine.file.unlink()
        else:
            log.info('use present %r', engine.file)
            log_dataset(dict(ds))
            return engine

    if engine.file is None:
        log.warning('connected to a transient in-memory database')


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
        conn.execute('PRAGMA application_id = %d' % application_id)

        log.debug('run create_all')
        Model.metadata.create_all(bind=conn)

    log.info('record git commit')
    log.debug('cwd: %r', root)
    get_stdout = functools.partial(_tools.check_output, cwd=str(root))

    try:
        dataset = {
            'title': 'Glottolog treedb',
            'git_commit': get_stdout(['git', 'rev-parse', 'HEAD']),
            'git_describe': get_stdout(['git', 'describe', '--tags', '--always']),
            # neither changes in index nor untracked files
            'clean': not get_stdout(['git', 'status', '--porcelain']),
            'exclude_raw': exclude_raw,
        }
    except Exception:
        log.exception('error running git command in %r', str(root))
        raise

    if not exclude_raw:
        log.info('load raw')
        with begin() as conn:
            log.debug('root: %r', root)
            raw.load(root, conn)

    if not (from_raw or exclude_raw):
        warnings.warn('2 tree reads required (use compare_with_raw() to verify)')

    log.debug('import module languoids')
    from . import languoids

    log.info('load languoids')
    with begin() as conn:
        root_or_bind = conn if from_raw else root
        log.debug('root_or_bind: %r', root_or_bind)

        pairs = languoids.iterlanguoids(root_or_bind)
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


def log_dataset(params, name=Dataset.__tablename__):
    log.info('git describe %(git_describe)r clean: %(clean)r', params)
    if not params['clean']:
        warnings.warn('%r not clean' % name)
    log.debug('%r.title: %r', name, params['title'])
    log.debug('%r.git_commit: %r', name, params['git_commit'])


def dump_sql(engine=ENGINE, filename=None, encoding=ENCODING):
    """Dump the engine database into a plain-text SQL file."""
    if filename is None:
        filename = engine.file_with_suffix('.sql').name
    path = _tools.path_from_filename(filename)
    log.info('dump sql to %r', path)

    with contextlib.closing(engine.raw_connection()) as dbapi_conn,\
         path.open('w', encoding=ENCODING) as f:
        n = 0
        for n, line in enumerate(dbapi_conn.iterdump(), 1):
            print(line, file=f)
            if not (n % 100000):
                log.debug('%d lines written', n)

    log.info('%d lines total', n)
    return path


def export(engine=ENGINE, filename=None, dialect=DIALECT, encoding=ENCODING,
           metadata=Model.metadata):
    """Write all tables to <tablename>.csv in <databasename>.zip."""
    log.info('export database')
    log.debug('engine: %r', engine)

    if filename is None:
        filename = engine.file_with_suffix('.zip').name

    log.info('write %r', filename)
    with engine.connect() as conn, zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in metadata.sorted_tables:
            log.info('export table %r', table.name)
            rows = table.select(bind=conn).execute()
            header = rows.keys()
            data = _tools.write_csv(None, rows, header=header,
                                    dialect=dialect, encoding=encoding)
            z.writestr('%s.csv' % table.name, data)

    log.info('database exported')
    return _tools.path_from_filename(filename)
