# export.py - sqlite3 database export functions

import contextlib
import datetime
import functools
import gzip
import hashlib
import logging
import warnings
import zipfile

import csv23

import sqlalchemy as sa

from .._globals import ENGINE, REGISTRY, DEFAULT_HASH

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
                  file=None, bind=ENGINE):
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


def print_schema(metadata=REGISTRY.metadata, *, file=None,
                 engine=ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql):
        print(sql.compile(dialect=engine.dialect),
              file=file)

    mock_engine = sa.create_mock_engine(engine.url, executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)


def print_query_sql(query=None, *,
                    literal_binds=True, pretty=True,
                    file=None, flush=True):
    """Print the literal SQL for the given query."""
    sql = get_query_sql(query, literal_binds=literal_binds, pretty=pretty)
    print(sql, file=file, flush=flush)


def get_query_sql(query=None,
                  *, literal_binds=True, pretty=False):
    """Return the literal SQL for the given query."""
    if query is None:
        from .. import queries

        query = queries.get_query()

    compiled = _backend.expression_compile(query, literal_binds=literal_binds)
    result = compiled.string

    if pretty and _backend.sqlparse is not None:
        result = _backend.sqlparse.format(result, reindent=True)
    return result


def backup(filename=None, *, as_new_engine=False,
           pages=0, engine=ENGINE):
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


def csv_zipfile(filename=None, *, exclude_raw=False,
                metadata=REGISTRY.metadata,
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
    with _backend.connect(bind=engine) as conn,\
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


def print_rows(query=None, *, format_=None,
               verbose=False, file=None, bind=ENGINE):
    if query is None:
        from .. import queries as _queries

        query = _queries.get_query()

    if not isinstance(query, sa.sql.base.Executable):
        # assume mappings
        rows = iter(query)
    else:
        if verbose:
            print(query, file=file)

        rows = _backend.iterrows(query, mappings=True, bind=bind)

    if format_ is not None:
        rows = map(format_.format_map, rows)

    for r in rows:
        print(r, file=file)


def write_csv(query=None, filename=None, *, verbose=False,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING, bind=ENGINE):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        from .. import queries as _queries

        query = _queries.get_query()

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


def hash_csv(query=None, *, hash_name: str = DEFAULT_HASH,
             dialect=csv23.DIALECT, encoding=csv23.ENCODING,
             raw: bool = False, bind=ENGINE):
    if query is None:
        from .. import queries as _queries

        query = _queries.get_query()

    with _backend.connect(bind=bind) as conn:
        result = conn.execute(query)

        header = list(result.keys())
        return hash_rows(result, header=header, hash_name=hash_name, raw=raw,
                         dialect=dialect, encoding=encoding)


def hash_rows(rows, *, hash_name: str = DEFAULT_HASH,
              header=None,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING,
              raw: bool = False):
    if hash_name is None:
        hash_name = DEFAULT_HASH

    log.info('hash rows with %r, csv header: %r', hash_name, header)
    result = hashlib.new(hash_name)
    assert hasattr(result, 'hexdigest')

    csv23.write_csv(result, rows, header=header,
                    dialect=dialect, encoding=encoding)

    if not raw:
        result = result.hexdigest()
    return result
