# fixex.py

from __future__ import unicode_literals

import logging

from sqlalchemy import delete, exists
from sqlalchemy.orm import aliased

from .. import ENGINE

from .models import Option, Value

__all__ = [
    'drop_duplicate_sources',
    'drop_duplicated_triggers',
    'drop_duplicated_crefs',
]


log = logging.getLogger(__name__)


def dropfunc(func, bind=ENGINE):
    def wrapper(bind=bind):
        log.info('execute %r', func.__name__)
        delete_rows = func()

        rows_deleted = bind.execute(delete_rows).rowcount

        log.info('%d rows deleted', rows_deleted)
        return rows_deleted

    return wrapper


@dropfunc
def drop_duplicate_sources():
    Other = aliased(Value)
    return delete(Value)\
        .where(exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'sources'))\
        .where(exists()
            .where(Other.file_id == Value.file_id)
            .where(Other.option_id == Value.option_id)
            .where(Other.value == Value.value)
            .where(Other.line < Value.line))


@dropfunc
def drop_duplicated_triggers():
    Other = aliased(Value)
    return delete(Value)\
        .where(exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'triggers'))\
        .where(exists()
            .where(Other.file_id == Value.file_id)
            .where(Other.option_id == Value.option_id)
            .where(Other.value == Value.value)
            .where(Other.line < Value.line))


@dropfunc
def drop_duplicated_crefs():
    Other = aliased(Value)
    return delete(Value)\
        .where(exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'classification')
            .where(Option.option.in_(('familyrefs', 'subrefs'))))\
        .where(exists()
            .where(Other.file_id == Value.file_id)
            .where(Other.option_id == Value.option_id)
            .where(Other.value == Value.value)
            .where(Other.line < Value.line))
