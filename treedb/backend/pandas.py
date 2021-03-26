# pandas convenience short-cut functions

import functools
import io
import logging
import operator
import warnings

from .. import _globals
from .. import _tools
from .. import backend as _backend

__all__ = ['pd_read_sql',
           'pd_read_json_lines']

PANDAS = None


log = logging.getLogger(__name__)


def _import_pandas():
    global PANDAS

    if PANDAS is None:
        try:
            import pandas as PANDAS
        except ImportError as e:
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


def pd_read_json_lines(*, order_by: str = _globals.LANGUOID_ORDER,
                       sort_keys: bool = True,
                       path_label: str = _globals.PATH_LABEL,
                       languoid_label: str = _globals.LANGUOID_LABEL,
                       buflines: int = 5_000,
                       bind=_globals.ENGINE, **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    from .. import queries

    query = queries.get_json_query(as_rows=False,
                                   load_json=False,
                                   order_by=order_by,
                                   sort_keys=sort_keys,
                                   path_label=path_label,
                                   languoid_label=languoid_label)

    with _backend.connect(bind=bind) as conn, io.StringIO() as buf:
        result = conn.execute(query)
        print_line = functools.partial(print, file=buf)
        df = None
        # TODO: try result.partitions()
        for lines in _tools.iterslices(result.scalars(), size=buflines):
            for line in lines:
                print_line(line)
            buf.seek(0)
            lines_df = PANDAS.read_json(buf, orient='record', lines=True, **kwargs)
            buf.seek(0)
            buf.truncate()
            if df is None:
                df = lines_df
            else:
                df = PANDAS.concat([df, lines_df], copy=False)

    df.rename(columns={'__path__': 'path'}, inplace=True)
    index = df['languoid'].map(operator.itemgetter('id')).rename('id')
    df.set_index(index, inplace=True, verify_integrity=True)
    return df
