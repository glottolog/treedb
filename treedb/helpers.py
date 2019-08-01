# helpers.py - short-cut functions

import functools

import sqlalchemy as sa

try:
    import pandas as pd
except ImportError:
    pd = None

from .backend import ENGINE

__all__ = ['text', 'select', 'count', 'read_sql']


text = functools.partial(sa.text, bind=ENGINE)

select = functools.partial(sa.select, bind=ENGINE)

count = sa.func.count

read_sql = None

if pd is not None:
    read_sql = functools.partial(pd.read_sql_query, con=ENGINE)
