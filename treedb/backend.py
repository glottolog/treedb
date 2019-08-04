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

from . import tools as _tools

from . import ENGINE, ROOT

__all__ = [
    'Model', 'print_schema',
    'Dataset',
    'Session',
    'load', 'dump_sql', 'export',
]


log = logging.getLogger(__name__)


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


def load(filename=ENGINE, root=ROOT, require=False, rebuild=False,
         exclude_raw=False, from_raw=None, force_delete=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    log.info('load database')

    if hasattr(filename, 'execute'):
        engine = filename
    else:
        ENGINE.file = filename
        engine = ENGINE

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
            found = sa.select([Dataset.exclude_raw], bind=engine).scalar()
        except Exception:
            msg = 'error reading %r'
            log.exception(msg, Dataset.__tablename__)
            warnings.warn(msg % Dataset.__tablename__)
            if force_delete:
                msg = 'force_delete %r'
                log.warning(msg, engine.file)
                warnings.warn(msg % engine.file)
                rebuild = True
            else:
                raise

        if found != exclude_raw:
            log.info('rebuild needed from exclude_raw mismatch')

        if rebuild or found != exclude_raw:
            log.info('rebuild database')
            log.debug('dispose engine %r', engine)
            engine.dispose()

            msg = 'delete present file: %r'
            log.warning(msg, engine.file)
            warnings.warn(msg % engine.file)
            engine.file.unlink()
        else:
            log.info('use present %r', engine.file)
            if log.level <= logging.INFO:
                git_describe = sa.select([Dataset.git_describe], bind=engine).scalar()
                log.info('git describe %r' % git_describe)
            return engine

    if engine.file is None:
        log.warning('connected to a transient in-memory database')

    if not (exclude_raw or from_raw):
        msg = '2 root reads required (use compare_with_raw() to verify)'
        log.warning(msg)
        warnings.warn(msg)

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

    dataset = {
        'title': 'Glottolog treedb',
        'git_commit': get_stdout(['git', 'rev-parse', 'HEAD']),
        'git_describe': get_stdout(['git', 'describe', '--tags', '--always']),
        # neither changes in index nor untracked files
        'clean': not get_stdout(['git', 'status', '--porcelain']),
        'exclude_raw': exclude_raw,
    }

    if not exclude_raw:
        log.info('load raw')
        with begin() as conn:
            log.debug('root: %r', root)
            raw.load(root, conn)

    log.debug('import module languoids')
    from . import languoids

    log.info('load languoids')
    if not (from_raw or exclude_raw):
        log.warning('must read tree 2 times (verify with compare_with_raw)')

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
    log.info('git describe %r', dataset['git_describe'])
    print(walltime)
    return engine


def dump_sql(engine=ENGINE, filename=None, encoding=ENCODING):
    if filename is None:
        filename = engine.file_with_suffix('.sql').name
    path = _tools.path_from_filename(filename)

    with contextlib.closing(engine.raw_connection()) as dbapi_conn,\
         path.open('w', encoding=ENCODING) as f:
        for line in dbapi_conn.iterdump():
            print(line, file=f)

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
