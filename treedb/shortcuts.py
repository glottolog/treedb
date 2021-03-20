# shortcuts.py - pandas convenience short-cut functions

import io
import logging
import operator
import warnings

from . import ENGINE

from . import backend as _backend

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


def pd_read_sql(sql=None, *args, con=ENGINE, **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    if sql is None:
        from . import queries

        sql = queries.get_query()

    with _backend.connect(bind=con) as conn:
        return PANDAS.read_sql_query(sql, *args, con=conn, **kwargs)


def pd_read_json_lines(bind=ENGINE, **kwargs):
    _import_pandas()

    if PANDAS is None:
        return None

    from . import queries

    with io.StringIO() as buf:
        queries.write_json_lines(buf, bind=bind)

        buf.seek(0)

        df = PANDAS.read_json(buf, orient='record', lines=True, **kwargs)

    index = df['languoid'].map(operator.itemgetter('id')).rename('id')
    df.set_index(index, inplace=True, verify_integrity=True)

    return df
