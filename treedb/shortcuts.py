# shortcuts.py - sqlalchemy and pandas convenience short-cut functions

import functools
import io
import logging
import operator
import warnings

import sqlalchemy as sa

from . import ENGINE

__all__ = ['count', 'select', 'text',
           'pd_read_sql',
           'pd_read_json_lines']

PANDAS = None


log = logging.getLogger(__name__)


count = sa.func.count


select = functools.partial(sa.select, bind=ENGINE)


text = functools.partial(sa.text, bind=ENGINE)


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

        sql = queries.get_query(bind=con)

    return PANDAS.read_sql_query(sql, *args, con=con, **kwargs)


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
