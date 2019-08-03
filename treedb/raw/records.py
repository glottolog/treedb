# records.py

import logging
import itertools

from .._compat import zip

import sqlalchemy as sa

from .. import ENGINE

from .. import tools as _tools

from .models import File, Option, Value

__all__ = ['iterrecords']

WINDOWSIZE = 500


log =  logging.getLogger(__name__)


def iterrecords(bind=ENGINE, windowsize=WINDOWSIZE, skip_unknown=True):
    """Yield (<path_part>, ...), <dict of <dicts of strings/string_lists>>) pairs."""
    log.info('read raw records')
    log.debug('bind: %r', bind)
    select_files = sa.select([File.path], bind=bind).order_by(File.id)
    # depend on no empty value files (save sa.outerjoin(File, Value) below)
    select_values = sa.select([
            Value.file_id, Option.section, Option.option,
            Option.is_lines, Value.value,
        ], bind=bind)\
        .select_from(sa.join(Value, Option))\
        .order_by(Value.file_id, Option.section, Value.line, Option.option)
    if skip_unknown:
        select_values.append_whereclause(Option.is_lines != None)

    groupby = (('file_id',), ('section',), ('option', 'is_lines'))
    groupby = itertools.starmap(_tools.groupby_attrgetter, groupby)
    groupby_file, groupby_section, groupby_option = groupby

    for in_slice in window_slices(File.id, size=windowsize, bind=bind):
        if log.level <= logging.DEBUG:
            where = in_slice(File.id).compile(compile_kwargs={'literal_binds': True}).string
            log.debug('fetch rows %r', where)
        files = select_files.where(in_slice(File.id)).execute().fetchall()
        # single thread: no isolation level concerns
        values = select_values.where(in_slice(Value.file_id)).execute().fetchall()
        # join by file_id total order index
        for (path,), (_, values) in zip(files, groupby_file(values)):
            record = {
                s: {o: [l.value for l in lines] if is_lines else next(lines).value
                   for (o, is_lines), lines in groupby_option(sections)}
                for s, sections in groupby_section(values)}
            yield tuple(path.split('/')), record


def window_slices(key_column, size=WINDOWSIZE, bind=ENGINE):
    """Yield where clause making function for key_column windows of size."""
    row_num = sa.func.row_number().over(order_by=key_column).label('row_num')
    select_keys = sa.select([key_column.label('key'), row_num]).alias()
    select_keys = sa.select([select_keys.c.key], bind=bind)\
        .where(select_keys.c.row_num % size == 0)

    keys = (k for k, in select_keys.execute())

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
