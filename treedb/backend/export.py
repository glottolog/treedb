# export.py - sqlite3 database export functions

import contextlib
import datetime
import functools
import gzip
import logging
import warnings
import zipfile

import csv23

import sqlalchemy as sa

from .. import ENGINE, REGISTRY

from .. import tools as _tools

from .. import backend as _backend

__all__ = ['backup',
           'dump_sql',
           'csv_zipfile']


log = logging.getLogger(__name__)


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

    with contextlib.closing(engine.raw_connection()) as dbapi_source,\
         contextlib.closing(result.raw_connection()) as dbapi_dest:
        log.debug('sqlite3.backup(%r)', dbapi_dest.connection)

        dbapi_dest.execute('PRAGMA synchronous = OFF')
        dbapi_dest.execute('PRAGMA journal_mode = MEMORY')

        with dbapi_dest.connection as dbapi_conn:
            dbapi_source.backup(dbapi_conn, pages=pages, progress=progress)

    log.info('database backup complete')
    if as_new_engine:
        _backend.set_engine(result)
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


def csv_zipfile(filename=None, *, exclude_raw=False, metadata=REGISTRY.metadata,
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
    with _backend.connect(engine) as conn,\
         zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in sorted_tables:
            if table.name in skip:
                log.debug('skip table %r', table.name)
                continue

            log.info('export table %r', table.name)
            rows = conn.execute(sa.select(table))
            header = list(rows.keys())

            date_time = datetime.datetime.now().timetuple()[:6]
            info = zipfile.ZipInfo(f'{table.name}.csv', date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED

            with z.open(info, 'w') as f:
                csv23.write_csv(f, rows, header=header,
                                dialect=dialect, encoding=encoding)

    log.info('database exported')
    return _tools.path_from_filename(filename)
