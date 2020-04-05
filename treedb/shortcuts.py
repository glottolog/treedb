# shortcuts.py - sqlalchemy, and pandas short-cut functions

import functools
import warnings

import sqlalchemy as sa

from . import ENGINE

__all__ = ['count', 'select', 'text',
           'pd_read_sql']

PANDAS = None


count = sa.func.count


select = functools.partial(sa.select, bind=ENGINE)


text = functools.partial(sa.text, bind=ENGINE)


def pd_read_sql(sql=None, *args, con=ENGINE, **kwargs):
    global PANDAS
    if PANDAS is None:
        try:
            import pandas as PANDAS
        except ImportError as e:
            warnings.warn(f'failed to import pandas: {e}')
            return None

    if sql is None:
        from . import queries
        sql = queries.get_query(bind=con)

    return PANDAS.read_sql_query(sql, *args, con=con, **kwargs)
