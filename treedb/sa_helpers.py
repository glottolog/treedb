# sq_helpers.py - sqlalchemy short-cut functions

import functools

import sqlalchemy as sa

from . import ENGINE

__all__ = ['text', 'select', 'count']


text = functools.partial(sa.text, bind=ENGINE)

select = functools.partial(sa.select, bind=ENGINE)

count = sa.func.count
