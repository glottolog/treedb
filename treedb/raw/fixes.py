# fixes.py

import logging

import sqlalchemy as sa
import sqlalchemy.orm

from .. import ENGINE

from .models import Option, Value

__all__ = ['drop_duplicate_sources',
           'drop_duplicated_triggers',
           'drop_duplicated_crefs',
           'update_countries',
           'update_wikidata_links']


log = logging.getLogger(__name__)


def dropfunc(func, *, bind=ENGINE):
    def wrapper(bind=bind):
        log.info('execute %r', func.__name__)

        delete_rows = func()

        rows_deleted = bind.execute(delete_rows).rowcount

        log.info('%d rows deleted', rows_deleted)
        return rows_deleted

    return wrapper


@dropfunc
def drop_duplicate_sources():
    Other = sa.orm.aliased(Value, name='other')
    return sa.delete(Value).where(sa.exists()
                               .where(Option.id == Value.option_id)
                               .where(Option.section == 'sources'))\
                        .where(sa.exists()
                               .where(Other.file_id == Value.file_id)
                               .where(Other.option_id == Value.option_id)
                               .where(Other.value == Value.value)
                               .where(Other.line < Value.line))


@dropfunc
def drop_duplicated_triggers():
    Other = sa.orm.aliased(Value, namme='other')
    return sa.delete(Value).where(sa.exists()
                               .where(Option.id == Value.option_id)
                               .where(Option.section == 'triggers'))\
                        .where(sa.exists()
                               .where(Other.file_id == Value.file_id)
                               .where(Other.option_id == Value.option_id)
                               .where(Other.value == Value.value)
                               .where(Other.line < Value.line))


@dropfunc
def drop_duplicated_crefs():
    Other = sa.orm.aliased(Value, name='other')
    return sa.delete(Value).where(sa.exists()
                               .where(Option.id == Value.option_id)
                               .where(Option.section == 'classification')
                               .where(Option.option.in_(('familyrefs',
                                                         'subrefs'))))\
                        .where(sa.exists()
                               .where(Other.file_id == Value.file_id)
                               .where(Other.option_id == Value.option_id)
                               .where(Other.value == Value.value)
                               .where(Other.line < Value.line))


def update_countries(bind=ENGINE):
    # https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
    # TODO: missing 'Saint Lucia (LC)'
    old_new = {'Cape Verde (CV)': 'Cabo Verde (CV)',
               'Czech Republic (CZ)': 'Czechia (CZ)',
               'Macedonia, Republic of (MK)': 'North Macedonia (MK)',
               'Swaziland (SZ)': 'Eswatini (SZ)'}

    query = sa.update(Value, bind=bind)\
        .where(sa.exists()
               .where(Option.id == Value.option_id)
               .where(Option.section == 'core')
               .where(Option.option == 'countries'))\
        .where(Value.value == sa.bindparam('old'))\
        .values(value=sa.bindparam('new'))

    for old, new in old_new.items():
        log.info('update countries: %r -> %r', old, new)
        rows_updated = query.execute(old=old, new=new).rowcount
        log.info('%d rows updated', rows_updated)


def update_wikidata_links(bind=ENGINE):
    query = sa.update(Value, bind=bind)\
        .where(sa.exists()
               .where(Option.id == Value.option_id)
               .where(Option.section == 'core')
               .where(Option.option == 'links'))\
        .where(Value.value.like('http://www.wikidata.org/%'))\
        .values(value=sa.func.replace(Value.value, 'http://', 'https://'))

    rows_updated = query.execute().rowcount
    log.info('%d rows updated', rows_updated)
