"""Optional pandas dependency convenience short-cut functions."""

import functools
import io
import logging
import typing
import warnings

from .. import _globals
from .. import _tools
from .. import backend as _backend

__all__ = ['pd_read_sql',
           'pd_read_json_lines']

PANDAS = None

JSON_BUFLINES = 5_000


log = logging.getLogger(__name__)


def _import_pandas():
    global PANDAS

    if PANDAS is None:
        try:
            import pandas as PANDAS
        except ImportError as e:  # pragma: no cover
            warnings.warn(f'failed to import pandas: {e}')
        else:
            log.info('pandas version: %s', PANDAS.__version__)


def pd_read_sql(sql=None, *args, con=_globals.ENGINE, **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    if sql is None:
        from .. import queries

        sql = queries.get_example_query()

    with _backend.connect(bind=con) as conn:
        return PANDAS.read_sql_query(sql, *args, con=conn, **kwargs)


def pd_read_json_lines(query,
                       *, buflines: int = JSON_BUFLINES,
                       bind=_globals.ENGINE,
                       **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    with _backend.connect(bind=bind) as conn:
        result = conn.execute(query)
        json_lines = result.scalars()
        return _pd_read_json_lines(json_lines, **kwargs)


def _pd_read_json_lines(json_lines: typing.Iterable[str],
                        *, buflines: int = JSON_BUFLINES,
                        concat_ignore_index: bool = False,
                        **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    with io.StringIO() as buf:
        print_line = functools.partial(print, file=buf)
        df = None
        for chunk in _tools.iterslices(json_lines, size=buflines):
            for line in chunk:
                print_line(line)
            buf.seek(0)

            lines_df = PANDAS.read_json(buf, lines=True, **kwargs)
            buf.seek(0)
            buf.truncate()

            if df is None:
                df = lines_df
            else:
                df = PANDAS.concat([df, lines_df],
                                   ignore_index=concat_ignore_index,
                                   copy=False)

    return df
