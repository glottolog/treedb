# records.py

import contextlib
import functools
import itertools
import logging

import sqlalchemy as sa

from .. import ENGINE

from .. import backend as _backend
from .. import tools as _tools

from .models import File, Option, Value

__all__ = ['iterrecords']

WINDOWSIZE = 500


log = logging.getLogger(__name__)


def iterrecords(*, ordered=True, progress_after=_tools.PROGRESS_AFTER,
                windowsize=WINDOWSIZE, skip_unknown=True, bind=ENGINE):
    """Yield (<path_part>, ...), <dict of <dicts of strings/string_lists>>) pairs."""
    try:
        dbapi_conn = bind.connection.connection
    except AttributeError:
        dbapi_conn = None
    log.info('start generating raw records from %r', dbapi_conn or bind)

    select_files = sa.select(File.path)
    # depend on no empty value files (save sa.outerjoin(File, Value) below)
    select_values = sa.select(Value.file_id, Option.section, Option.option,
                               Option.is_lines, Value.value)

    values_from = sa.join(Value, Option)

    if ordered in (True, False, 'file'):
        key_column = File.id
        value_key = Value.file_id
    elif ordered == 'id':
        values_from = values_from.join(File)
        key_column = value_key = File.glottocode
    elif ordered == 'path':
        values_from = values_from.join(File)
        key_column = value_key = File.path
    else:
        raise ValueError(f'ordered={ordered!r} not implememted')
    log.info('ordered: %r', ordered)

    select_files = select_files.order_by(key_column)

    select_values = select_values.select_from(values_from)\
                    .order_by(value_key, 'section', Value.line, 'option')

    if skip_unknown:
        select_values = select_values.where(Option.is_lines != None)

    groupby = (('file_id',), ('section',), ('option', 'is_lines'))
    groupby = itertools.starmap(_tools.groupby_attrgetter, groupby)
    groupby_file, groupby_section, groupby_option = groupby

    n = 0
    with _backend.connect(bind) as conn:
        for in_slice in window_slices(key_column, size=windowsize, bind=conn):
            if log.level <= logging.DEBUG:
                where = _backend.expression_compile(in_slice(key_column))
                log.debug('fetch rows %r', where.string)

            files = conn.execute(select_files.where(in_slice(key_column))).all()
            # single thread: no isolation level concerns
            values = conn.execute(select_values.where(in_slice(value_key))).all()
            # join in-memory by file_id total order index
            path_values = zip(files, groupby_file(values))

            for count, ((path,), (_, values)) in enumerate(path_values, 1):
                record = {
                    s: {o: [l.value for l in lines] if is_lines else next(lines).value
                       for (o, is_lines), lines in groupby_option(sections)}
                    for s, sections in groupby_section(values)}
                yield tuple(path.split('/')), record

            n += count
            if not (n % progress_after):
                log.info('%s raw records generated', f'{n:_d}')

    log.info('%s raw records total', f'{n:_d}')


def window_slices(key_column, *, size=WINDOWSIZE, bind=ENGINE):
    """Yield where clause making function for key_column windows of size.

    adapted from https://github.com/sqlalchemy/sqlalchemy/wiki/RangeQuery-and-WindowedRangeQuery
    """
    if bind.dialect.dbapi.sqlite_version_info < (3, 25):
        iterkeys_func = iterkeys_compat
    else:
        iterkeys_func = iterkeys

    log.info('fetch %r slices for window of %d', str(key_column.expression), size)
    keys = iterkeys_func(key_column, size=size, bind=bind)

    try:
        end = next(keys)
    except StopIteration:
        yield lambda c: sa.and_()
        return

    # right-inclusive indexes for windows of given size for continuous keys
    yield lambda c, end=end: (c <= end)
    last = end

    for end in keys:
        yield lambda c, last=last, end=end: sa.and_(c > last, c <= end)
        last = end

    yield lambda c, end=end: (c > end)


def iterkeys(key_column, *, size=WINDOWSIZE, bind=ENGINE):
    row_num  = sa.func.row_number().over(order_by=key_column).label('row_num')
    select_all_keys = sa.select(key_column.label('key'), row_num)\
                      .alias('key_ord')

    select_keys = sa.select(select_all_keys.c.key)\
                  .where((select_all_keys.c.row_num % size) == 0)

    log.debug('SELECT every %d-th %r using row_number() window function',
              size, str(key_column.expression))

    with _backend.connect(bind) as conn,\
         contextlib.closing(conn.execute(select_keys)) as result:
        yield from result.scalars()


# Python 3.6 compat
def iterkeys_compat(key_column, *, size=WINDOWSIZE, bind=ENGINE):
    select_keys = sa.select(key_column.label('key'))\
                  .order_by(key_column)

    log.debug('SELECT every %r and yield every %d-th one using cursor iteration',
              str(key_column.expression), size)

    with _backend.connect(bind) as conn,\
         contextlib.closing(conn.execute(select_keys)) as cursor:
        for keys in iter(functools.partial(cursor.fetchmany, size), []):
            last, = keys[-1]
            yield last
