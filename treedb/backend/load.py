"""Import data into SQLite3 database."""

import datetime
import functools
import logging
import time
import warnings

import sqlalchemy as sa

from .. import _globals
from .. import _tools
from .. import files as _files

from .. import backend as _backend

from . import models as _models
from . import views as _views

__all__ = ['main']


log = logging.getLogger(__name__)


def get_root(repo_root, *, default,
             treepath=_files.TREE_IN_ROOT):
    if repo_root is not None:
        root = _files.set_root(repo_root, treepath=treepath)
    else:
        root = default

    log.info('load database from %r', root)
    if not root.exists():
        log.error('root does not exist')
        raise RuntimeError(f'tree root not found: {root!r}')
    return root


def get_from_raw(from_raw, *, exclude_raw: bool):
    if exclude_raw and from_raw:  # pragma: no cover
        log.error('incompatible exclude_raw=%r and from_raw=%r', exclude_raw, from_raw)
        raise ValueError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw
    return from_raw


def get_engine(filename_or_engine, *, require: bool):
    if hasattr(filename_or_engine, 'execute'):
        return filename_or_engine
    return _backend.set_engine(filename_or_engine, require=require)


def get_dataset(engine, *, exclude_raw: bool, strict: bool):
    dataset = None

    if engine.file is None:
        log.warning('connected to a transient in-memory database')
        dataset = _models.Dataset.get_dataset(bind=engine, strict=True)
    elif engine.file_size():
        dataset = _models.Dataset.get_dataset(bind=engine, strict=strict)

        if dataset is None:
            warnings.warn(f'force delete {engine.file!r}')
        elif dataset.exclude_raw != bool(exclude_raw):  # pragma: no cover
            dataset = None
            log.warning('rebuild needed from exclude_raw mismatch')

    return dataset


def main(filename=_globals.ENGINE, repo_root=None,
         *, treepath=_files.TREE_IN_ROOT,
         metadata=_globals.REGISTRY.metadata,
         require: bool = False,
         rebuild: bool = False,
         from_raw: bool = None,
         exclude_raw: bool = False,
         exclude_views: bool = False,
         force_rebuild: bool = False,
         _only_create_tables: bool = False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return engine."""
    kwargs = {'root': get_root(repo_root, default=_globals.ROOT, treepath=treepath),
              'from_raw': get_from_raw(from_raw, exclude_raw=exclude_raw)}

    engine = get_engine(filename, require=require)

    dataset = get_dataset(engine,
                          exclude_raw=exclude_raw,
                          strict=not force_rebuild and not _only_create_tables)

    if dataset is None or rebuild or _only_create_tables:
        log.info('build new database' if dataset is None else 'rebuild database')
        engine.dispose()
        if engine.file_size():
            warnings.warn(f'delete present file: {engine.file!r}')
            engine.file.unlink()

        log.info('create %d tables from %r', len(metadata.tables), metadata)
        with _backend.connect(bind=engine) as conn:
            create_tables(metadata, conn=conn,
                          exclude_raw=exclude_raw,
                          exclude_views=exclude_views)
            log.info('COMMIT create_all: %r', conn)
            conn.commit()

        if _only_create_tables:
            return engine

        log.debug('start load timer')
        start = time.time()
        with _backend.connect(bind=engine, pragma_bulk_insert=True) as conn:
            dataset = load(metadata, conn=conn,
                           exclude_raw=exclude_raw,
                           **kwargs)
        walltime = datetime.timedelta(seconds=time.time() - start)
        log.debug('load timer stopped')
        print(walltime)

        log.info('database loaded')
    else:
        log.info('use present %r', engine)

    _models.Dataset.log_dataset(dataset)
    pdc = _models.Producer.get_producer(bind=engine)
    _models.Producer.log_producer(pdc)

    return engine


def create_tables(metadata, *, conn,
                  exclude_raw: bool, exclude_views: bool):
    # import here to register models for create_all()
    log.debug('import module %s.models', __package__)
    from .. import models

    assert models is not None

    if not exclude_raw:
        log.debug('import module %s.raw', __package__)

        from .. import raw

        assert raw is not None

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


def load(metadata, *, conn, root,
         from_raw: bool, exclude_raw: bool):
    log.info('record git commit in %r', root)
    # pre-create dataset to added as final item marking completeness
    dataset = make_dataset(root, exclude_raw=exclude_raw)
    _models.Dataset.log_dataset(dataset)

    log.info('write %r', _models.Producer.__tablename__)
    write_producer(conn, name=__package__.partition('.')[0])

    log.info('load config/*.ini into %r', _models.Config.__tablename__)
    version = import_configs(conn, root=root)
    log.info('version from %r: %r', _models.Config.__tablename__, version)
    dataset['version'] = version

    log.info('COMMIT producer and configs: %r', conn)
    conn.commit()

    if not exclude_raw:
        log.info('load raw')
        import_raw(conn, root=root)

        log.info('COMMIT raw: %r', conn)
        conn.commit()

    if not (from_raw or exclude_raw):  # pragma: no cover
        warnings.warn('2 tree reads required (use compare_with_files() to verify)')

    log.info('load languoids')
    import_languoids(conn, root=root,
                     source='raw' if from_raw else 'files')

    log.info('COMMIT languoids: %r', conn)
    conn.commit()

    log.info('write %r: %r', _models.Dataset.__tablename__, dataset['title'])
    write_dataset(conn, dataset=dataset)

    log.info('COMMIT dataset: %r', conn)
    conn.commit()

    return dataset


def import_configs(conn, *, root):
    insert_config = functools.partial(conn.execute, sa.insert(_models.Config))
    for filename, cfg in _files.iterconfigs(root):
        get_line = _tools.next_count(start=1)
        params = [{'filename': filename, 'section': section, 'option': option,
                   'value': value.strip(), 'line': get_line()}
                  for section, sec in cfg.items()
                  for option, value in sec.items()
                  if value.strip()]

        log.debug('insert %d values for %r', len(params), filename)
        insert_config(params)

    select_version = (sa.select(_models.Config.value)
                      .filter_by(filename='publication.ini',
                                 section='zenodo', option='version'))
    return conn.execute(select_version).scalar_one_or_none()


def make_dataset(root, *, exclude_raw: bool):
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


def write_dataset(conn, *, dataset):
    log.debug('dataset: %r', dataset)
    conn.execute(sa.insert(_models.Dataset), dataset)


def write_producer(conn, *, name: str):
    from .. import __version__

    params = {'name': name, 'version': __version__}
    _models.Producer.log_producer(params)
    conn.execute(sa.insert(_models.Producer), params)


def import_raw(conn, *, root):
    log.debug('import target module %s.raw.import_models', __package__)

    from ..raw import import_models

    log.debug('root: %r', root)

    import_models.main(root, conn=conn)


def import_languoids(conn, *, root, source: str):
    log.debug('import source module %s.languoids', __package__)

    from .. import export

    log.debug('import target module %s.import_models', __package__)

    from .. import import_models

    if source == 'files':
        order_by = True
        root_or_bind = root
    elif source == 'raw':
        # insert languoids in Glottocode order when reading from raw
        order_by = 'id'
        root_or_bind = conn
    else:  # pragma: no cover
        ValueError(f'unknown source: {source!r}')
    log.debug('root_or_bind: %r', root_or_bind)

    pairs = export.iterlanguoids(source,
                                 order_by=order_by,
                                 root=root, bind=conn)

    import_models.main(pairs, conn=conn)
