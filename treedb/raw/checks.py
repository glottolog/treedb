# checks.py

import logging

import csv23

import sqlalchemy as sa

from .. import queries as _queries

from .models import File, Option, Value

__all__ = ['checksum']


log = logging.getLogger(__name__)


def checksum(*, weak=False, name=None,
             dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    kind = {True: 'weak', False: 'strong', 'unordered': 'unordered'}[weak]
    log.info('calculate %r raw checksum', kind)

    if weak:
        select_rows = sa.select(File.path,
                                Option.section, Option.option,
                                Value.value)\
                      .join_from(File, Value).join(Option)\

        order = ['path', 'section', 'option']
        if weak == 'unordered':
            order.append(Value.value)
        else:
            order.append(Value.line)
        select_rows = select_rows.order_by(*order)

    else:
        select_rows = sa.select(File.path, File.sha256)\
                      .order_by('path')

    hash_ = _queries.hash_csv(select_rows, raw=True, name=name,
                               dialect=dialect, encoding=encoding)

    logging.debug('%s: %r', hash_.name, hash_.hexdigest())
    return f'{kind}:{hash_.name}:{hash_.hexdigest()}'
