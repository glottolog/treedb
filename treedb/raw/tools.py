# tools.py

import logging

import sqlalchemy as sa

from .. import queries as _queries

from .models import Option, Value

__all__ = ['print_stats']


log = logging.getLogger(__name__)


def print_stats():
    log.info('fetch statistics')

    select_nvalues = sa.select(Option.section, Option.option,
                               sa.func.count().label('n'))\
                     .join_from(Option, Value)\
                     .group_by(Option.section, Option.option)\
                     .order_by('section', sa.desc('n'))

    template = '{section:<22} {option:<22} {n:,}'

    _queries.print_rows(select_nvalues, format_=template)
