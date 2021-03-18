# load.py - load sqlite3 database

import contextlib
import datetime
import functools
import logging
import time
import warnings

import sqlalchemy as sa

from .. import (ENGINE, ROOT,
                REGISTRY as registry)

from .. import files as _files
from .. import tools as _tools
from .. import views as _views

from .. import backend as _backend

from .models import Dataset, Producer

__all__ = ['load']


log = logging.getLogger(__name__)


def load(filename=ENGINE, repo_root=None, *,
         treepath=_files.TREE_IN_ROOT,
         require=False, rebuild=False,
         exclude_raw=False, from_raw=None,
         exclude_views=False,
         force_rebuild=False,
         metadata=registry.metadata):
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
        engine = _backend.set_engine(filename, require=require)

    if engine.file is None:
        log.warning('connected to a transient in-memory database')

        ds = Dataset.get_dataset(bind=engine, strict=True)
    elif engine.file_size():
        ds = Dataset.get_dataset(bind=engine, strict=not force_rebuild)
        if ds is None:
            warnings.warn(f'force delete {engine.file!r}')
        elif ds.exclude_raw != bool(exclude_raw):
            ds = None
            log.warning('rebuild needed from exclude_raw mismatch')
    else:
        ds = None

    if ds is None or rebuild:
        log.info('build new database' if ds is None else 'rebuild database')
        engine.dispose()
        if engine.file_size():
            warnings.warn(f'delete present file: {engine.file!r}')
            engine.file.unlink()
    else:
        log.info('use present %r', engine)

    if ds is not None and not rebuild:
        Dataset.log_dataset(ds)
        pdc = Producer.get_producer(bind=engine)
        Producer.log_producer(pdc)
    else:
        dataset = _load(metadata, engine=engine, root=root, 
                        from_raw=from_raw, exclude_raw=exclude_raw,
                        exclude_views=exclude_views)
        log.info('database loaded')
        Dataset.log_dataset(dataset)

    return engine


@contextlib.contextmanager
def begin(*, bind, pragma_bulk_insert=True):
    """Enter transaction: log boundaries, apply insert optimization, return connection."""
    with bind.begin() as conn:
        dbapi_conn = conn.connection.connection
        log.debug('begin transaction on %r', dbapi_conn)

        if pragma_bulk_insert:
            conn.execute(sa.text('PRAGMA synchronous = OFF'))
            conn.execute(sa.text('PRAGMA journal_mode = MEMORY'))

        yield conn

    log.debug('end transaction on %r', dbapi_conn)


def create_tables(metadata, *, conn, exclude_raw, exclude_views):
    # import here to register models for create_all()
    log.debug('import module %s.models', __package__)
    from .. import models

    if not exclude_raw:
        log.debug('import module %s.raw', __package__)

        from .. import raw

    if not exclude_views:
        log.info('prepare %d views', len(_views.VIEW_REGISTRY))

    try:
        _views.create_all_views(clear=exclude_views)
    except Exception:  # pragma: no cover
        log.exception('error running %s.views.create_all_views(clear=%r)',
                      __package__, exclude_views)

    application_id = sum(ord(c) for c in Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    log.debug('set application_id = %r', application_id)
    conn.execute(sa.text(f'PRAGMA application_id = {application_id:d}'))

    log.debug('run create_all')
    metadata.create_all(bind=conn)


def _load(metadata, *, engine, root, from_raw, exclude_raw, exclude_views):
    log.debug('start load timer')
    start = time.time()

    log.info('create %d tables from %r', len(metadata.tables), metadata)
    with begin(bind=engine) as conn:
        create_tables(metadata, conn=conn, exclude_raw=exclude_raw, exclude_views=exclude_views)

    log.info('record git commit in %r', root)
    dataset = make_dataset(root, exclude_raw=exclude_raw)
    Dataset.log_dataset(dataset)

    log.info('write %r', Producer.__tablename__)
    with begin(bind=engine) as conn:
        write_producer(conn, name=__package__.partition('.')[0])

    if not exclude_raw:
        log.info('load raw')
        with begin(bind=engine) as conn:
            load_raw(conn, root=root)

    if not (from_raw or exclude_raw):  # pragma: no cover
        warnings.warn('2 tree reads required (use compare_with_files() to verify)')

    log.info('load languoids')
    with begin(bind=engine) as conn:
        load_languoids(conn, root=root, from_raw=from_raw)

    log.info('write %r: %r', Dataset.__tablename__, dataset['title'])
    with begin(bind=engine) as conn:
        write_dataset(conn, dataset=dataset)

    walltime = datetime.timedelta(seconds=time.time() - start)
    log.debug('load timer stopped')
    print(walltime)
    return dataset


def make_dataset(root, *, exclude_raw):
    run = functools.partial(_tools.run, cwd=str(root), check=True,
                            capture_output=True, unpack=True)

    try:
        dataset = {'title': 'Glottolog treedb',
                   'git_commit': run(['git', 'rev-parse', 'HEAD']),
                   'git_describe': run(['git', 'describe', '--tags', '--always']),
                   # neither changes in index nor untracked files
                   'clean': not run(['git', 'status', '--porcelain']),
                   'exclude_raw': exclude_raw}
    except Exception as e:  # pragma: no cover
        log.exception('error running git command in %r', str(root))
        raise RuntimeError(f'failed to get info for dataset: {e}') from e
    else:
        log.info('identified dataset')
        return dataset


def write_producer(conn, *, name):
    from .. import __version__

    params = {'name': name, 'version': __version__}
    Producer.log_producer(params)
    conn.execute(sa.insert(Producer), params)


def load_raw(conn, *, root):
    from .. import raw

    log.debug('root: %r', root)
    raw.load(root, conn)


def load_languoids(conn, *, root, from_raw):
    log.debug('import module languoids')

    from .. import languoids

    log.debug('import module %s.load_models', __package__)

    from .. import load_models

    root_or_bind = conn if from_raw else root
    log.debug('root_or_bind: %r', root_or_bind)

    pairs = languoids.iterlanguoids(root_or_bind, from_raw=from_raw)
    load_models.load(pairs, conn)


def write_dataset(conn, *, dataset):
    log.debug('dataset: %r', dataset)
    conn.execute(sa.insert(Dataset), dataset)
