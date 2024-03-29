"""SQLite3 database export functions."""

import contextlib
import datetime
import functools
import gzip
import hashlib
import logging
import pprint
import sys
import typing
import warnings
import zipfile

import csv23

import sqlalchemy as sa

from .. import _globals
from .. import _tools
from .. import backend as _backend

from .models import Dataset, Producer

__all__ = ['print_dataset',
           'print_schema',
           'print_query_sql',
           'get_query_sql',
           'backup',
           'dump_sql',
           'csv_zipfile',
           'print_rows',
           'write_csv', 'hash_csv', 'hash_rows']


log = logging.getLogger(__name__)


def print_dataset(*, ignore_dirty: bool = False,
                  file=None,
                  bind=_globals.ENGINE):
    with _backend.connect(bind=bind) as conn:
        dataset = (conn.execute(sa.select(Dataset))
                   .mappings()
                   .one())
        producer = (conn.execute(sa.select(Producer))
                    .mappings()
                    .one())
    Dataset.log_dataset(dataset, ignore_dirty=ignore_dirty,
                        also_print=True, print_file=file)
    Producer.log_producer(producer,
                          also_print=True, print_file=file)


def print_schema(metadata=_globals.REGISTRY.metadata, /, *,
                 file=None,
                 engine=_globals.ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql, *_, **__):
        print(sql.compile(dialect=engine.dialect),
              file=file)

    mock_engine = sa.create_mock_engine(engine.url, executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)


def print_query_sql(query=None, /, *, literal_binds: bool = True,
                    pretty: bool = True,
                    file=None, flush: bool = True):
    """Print the literal SQL for the given query."""
    sql = get_query_sql(query, literal_binds=literal_binds, pretty=pretty)
    print(sql, file=file, flush=flush)


def get_query_sql(query=None, /, *, literal_binds: bool = True,
                  pretty: bool = False):
    """Return the literal SQL for the given query."""
    if query is None:
        from .. import queries

        query = queries.get_example_query()

    compiled = _backend.expression_compile(query, literal_binds=literal_binds)
    result = compiled.string

    if pretty and _backend.sqlparse is not None:
        result = _backend.sqlparse.format(result, reindent=True)
    return result


def backup(filename=None, /, *, as_new_engine: bool = False,
           pages: int = 0,
           engine=_globals.ENGINE):
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

    with contextlib.closing(engine.raw_connection()) as source_fairy,\
         contextlib.closing(result.raw_connection()) as dest_fairy:  # noqa: E231
        log.debug('sqlite3.backup(%r)', dest_fairy.driver_connection)

        dest_fairy.execute('PRAGMA synchronous = OFF')
        dest_fairy.execute('PRAGMA journal_mode = MEMORY')

        with dest_fairy.driver_connection as dbapi_conn:
            source_fairy.backup(dbapi_conn, pages=pages, progress=progress)

    log.info('database backup complete')
    if as_new_engine:
        _backend.set_engine(result)
    return result


def dump_sql(filename=None, /, *,
             progress_after: int = 100_000,
             encoding: str = _tools.ENCODING,
             engine=_globals.ENGINE):
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
    with contextlib.closing(engine.raw_connection()) as dbapi_fairy,\
         open_path('wt', encoding=encoding) as f:  # noqa: E231
        for n, line in enumerate(dbapi_fairy.iterdump(), start=1):
            print(line, file=f)
            if not (n % progress_after):
                log.info('%s lines written', f'{n:_d}')

    log.info('%s lines total', f'{n:_d}')
    return path


def csv_zipfile(filename=None, /, *, exclude_raw: bool = False,
                metadata=_globals.REGISTRY.metadata,
                dialect=csv23.DIALECT, encoding: str = csv23.ENCODING,
                engine=_globals.ENGINE):
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
    with _backend.connect(bind=engine) as conn,\
         zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:  # noqa: E231
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

    log.info('database export complete.')
    return _tools.path_from_filename(filename)


def print_rows(query=None, /, *, file=None,
               pretty: bool = False,
               format_: typing.Optional[str] = None,
               verbose: bool = False,
               mappings: bool = True,
               bind=_globals.ENGINE):
    if query is None:
        from .. import queries as _queries

        query = _queries.get_example_query()

    if not isinstance(query, sa.sql.base.Executable):
        # assume mappings
        rows = iter(query)
    else:
        if verbose:
            print(query, file=file)

        rows = _backend.iterrows(query,
                                 mappings=(format_ is not None) or mappings,
                                 bind=bind)

    print_func = functools.partial(print, file=file)
    if format_ is not None:
        rows = map(format_.format_map, rows)
    elif pretty:
        rows = map(dict if mappings else tuple, rows)
        print_func = PrettyPrinter(file).pprint

    for r in rows:
        print_func(r)


class PrettyPrinter(pprint.PrettyPrinter):

    def __init__(self, stream, /, *,
                 sort_dicts: bool = False,
                 **kwargs) -> None:
        if sys.version_info < (3, 8):
            if not sort_dicts:
                warnings.warn(f'sort_dicts={sort_dicts!r} not available')
            del sort_dicts
        else:
            kwargs['sort_dicts'] = sort_dicts
        if sys.version_info >= (3, 10):
            kwargs.setdefault('underscore_numbers', True)
        super().__init__(stream=stream, **kwargs)


def write_csv(query=None, /, filename=None, *,
              verbose: bool = False,
              dialect=csv23.DIALECT, encoding: str = csv23.ENCODING,
              bind=_globals.ENGINE):
    """Write get__example_query() query (or given query) to CSV, return filename."""
    if query is None:
        from .. import queries as _queries

        query = _queries.get_example_query()

    if filename is None:
        filename = bind.file_with_suffix('.query.csv').name
    filename = _tools.path_from_filename(filename)

    log.info('write csv: %r', filename)
    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'delete present file: {path!r}')
        path.unlink()

    if verbose:
        print(query)

    with _backend.connect(bind=bind) as conn:
        result = conn.execute(query)

        header = list(result.keys())
        log.info('csv header: %r', header)
        return csv23.write_csv(filename, result, header=header,
                               dialect=dialect, encoding=encoding,
                               autocompress=True)


def hash_csv(query=None, /, *, hash_name: str = _globals.DEFAULT_HASH,
             dialect=csv23.DIALECT, encoding: str = csv23.ENCODING,
             raw: bool = False,
             bind=_globals.ENGINE):
    if query is None:
        from .. import queries as _queries

        query = _queries.get_example_query()

    with _backend.connect(bind=bind) as conn:
        result = conn.execute(query)

        header = list(result.keys())
        return hash_rows(result, header=header, hash_name=hash_name, raw=raw,
                         dialect=dialect, encoding=encoding)


def hash_rows(rows, /, *, hash_name: str = _globals.DEFAULT_HASH,
              header=None,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING,
              raw: bool = False):
    if hash_name is None:
        hash_name = _globals.DEFAULT_HASH

    log.info('hash rows with %r, csv header: %r', hash_name, header)
    result = hashlib.new(hash_name)
    assert hasattr(result, 'hexdigest')

    csv23.write_csv(result, rows, header=header,
                    dialect=dialect, encoding=encoding)

    if not raw:
        result = result.hexdigest()
    return result
