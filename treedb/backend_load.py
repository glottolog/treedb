# backend_load.py - load sqlite3 database

import contextlib
import datetime
import functools
import logging
import time
import warnings

import sqlalchemy as sa

from . import (tools as _tools,
               files as _files,
               views as _views)

from .backend import set_engine, Model, Dataset, Producer

from . import ROOT, ENGINE

__all__ = ['load']


log = logging.getLogger(__name__)


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
        log.info('prepare %d views', len(_views.REGISTRY))

        try:
            _views.create_all_views()
        except Exception:
            log.exception('error running %s.views.create_all_views()', __package__)

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
