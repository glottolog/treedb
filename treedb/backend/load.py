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

from .. import _tools
from .. import files as _files

from .. import backend as _backend

from . import models  as _models
from . import views as _views

__all__ = ['main']


log = logging.getLogger(__name__)


def get_root(repo_root, *, default):
    if repo_root is not None:
        root = _files.set_root(repo_root, treepath=treepath)
    else:
        root = default

    log.info('load database from %r', root)
    if not root.exists():
        log.error('root does not exist')
        raise RuntimeError(f'tree root not found: {root!r}')
    return root


def get_from_raw(from_raw, *, exclude_raw):
    if exclude_raw and from_raw:
        log.error('incompatible exclude_raw=%r and from_raw=%r', exclude_raw, from_raw)
        raise ValueError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw
    return from_raw


def get_engine(filename_or_engine, *, require):
    if hasattr(filename_or_engine, 'execute'):
        return filename_or_engine
    return _backend.set_engine(filename_or_engine, require=require)


def get_dataset(engine, *, exclude_raw, force_rebuild):
    dataset = None

    if engine.file is None:
        log.warning('connected to a transient in-memory database')
        dataset = _models.Dataset.get_dataset(bind=engine, strict=True)
    elif engine.file_size():
        dataset = _models.Dataset.get_dataset(bind=engine,
                                              strict=not force_rebuild)

        if dataset is None:
            warnings.warn(f'force delete {engine.file!r}')
        elif dataset.exclude_raw != bool(exclude_raw):
            dataset = None
            log.warning('rebuild needed from exclude_raw mismatch')

    return dataset


def main(filename=ENGINE, repo_root=None, *,
         treepath=_files.TREE_IN_ROOT,
         require=False, rebuild=False,
         from_raw=None,
         exclude_raw=False,
         exclude_views=False,
         force_rebuild=False,
         metadata=registry.metadata):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    root = get_root(repo_root, default=ROOT)

    from_raw = get_from_raw(from_raw, exclude_raw=exclude_raw)

    engine = get_engine(filename, require=require)

    dataset = get_dataset(engine,
                          exclude_raw=exclude_raw,
                          force_rebuild=force_rebuild)

    if dataset is None or rebuild:
        log.info('build new database' if dataset is None else 'rebuild database')
        engine.dispose()
        if engine.file_size():
            warnings.warn(f'delete present file: {engine.file!r}')
            engine.file.unlink()

        dataset = load(metadata,
                       bind=engine,
                       root=root,
                       from_raw=from_raw,
                       exclude_raw=exclude_raw,
                       exclude_views=exclude_views)

        log.info('database loaded')
    else:
        log.info('use present %r', engine)

    _models.Dataset.log_dataset(dataset)
    pdc = _models.Producer.get_producer(bind=engine)
    _models.Producer.log_producer(pdc)

    return engine


def create_tables(metadata, *, conn, exclude_raw, exclude_views):
    # import here to register models for create_all()
    log.debug('import module %s.models', __package__)
    from .. import models

    if not exclude_raw:
        log.debug('import module %s.raw', __package__)

        from .. import raw

    if not exclude_views:
        log.info('prepare %d views', len(_views.REGISTERED))

    try:
        _views.create_all_views(clear=exclude_views)
    except Exception:  # pragma: no cover
        log.exception('error running %s.views.create_all_views(clear=%r)',
                      __package__, exclude_views)

    application_id = sum(ord(c) for c in _models.Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    log.debug('set application_id = %r', application_id)
    conn.execute(sa.text(f'PRAGMA application_id = {application_id:d}'))

    log.debug('run create_all')
    metadata.create_all(bind=conn)


@contextlib.contextmanager
def begin(*, bind, pragma_bulk_insert=True):
    """Enter transaction: log boundaries, apply insert optimization, return connection."""
    with _backend.connect(bind) as conn, conn.begin():
        dbapi_conn = conn.connection.connection
        log.debug('begin transaction on %r', dbapi_conn)

        if pragma_bulk_insert:
            conn.execute(sa.text('PRAGMA synchronous = OFF'))
            conn.execute(sa.text('PRAGMA journal_mode = MEMORY'))

        yield conn

    log.debug('end transaction on %r', dbapi_conn)


def load(metadata, *, bind, root, from_raw, exclude_raw, exclude_views):
    log.debug('start load timer')
    start = time.time()

    log.info('create %d tables from %r', len(metadata.tables), metadata)
    with begin(bind=bind) as conn:
        create_tables(metadata, conn=conn,
                      exclude_raw=exclude_raw,
                      exclude_views=exclude_views)

    log.info('record git commit in %r', root)
    # pre-create dataset to added as final item marking completeness
    dataset = make_dataset(root, exclude_raw=exclude_raw)
    _models.Dataset.log_dataset(dataset)

    log.info('write %r', _models.Producer.__tablename__)
    with begin(bind=bind) as conn:
        write_producer(conn, name=__package__.partition('.')[0])

    if not exclude_raw:
        log.info('load raw')
        with begin(bind=bind) as conn:
            load_raw(conn, root=root)

    if not (from_raw or exclude_raw):  # pragma: no cover
        warnings.warn('2 tree reads required (use compare_with_files() to verify)')

    log.info('load languoids')
    with begin(bind=bind) as conn:
        load_languoids(conn, root=root, from_raw=from_raw)

    log.info('write %r: %r', _models.Dataset.__tablename__, dataset['title'])
    with begin(bind=bind) as conn:
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
                   # clean = neither changes in index nor untracked files
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
    _models.Producer.log_producer(params)
    conn.execute(sa.insert(_models.Producer), params)


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
    conn.execute(sa.insert(_models.Dataset), dataset)
