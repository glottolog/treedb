# sqlite_master.py - read sqlite3 sqlite_master tables

import sqlalchemy as sa

from .. import ENGINE

__all__ = ['print_table_sql',
           'select_table_sql',
           'select_table_nrows',
           'select_tables_nrows',
           'select_tables',
           'select_views']


# see also: https://www.sqlite.org/faq.html#q7
sqlite_master = sa.table('sqlite_master',
                         sa.column('type', sa.Text),
                         sa.column('name', sa.Text),
                         sa.column('tbl_name', sa.Text),
                         sa.column('rootpage', sa.Integer),
                         sa.column('sql', sa.Text))


sqlite_temp_master = sa.table('sqlite_temp_master',
                              sa.column('type', sa.Text),
                              sa.column('name', sa.Text),
                              sa.column('tbl_name', sa.Text),
                              sa.column('rootpage', sa.Integer),
                              sa.column('sql', sa.Text))


def print_table_sql(model_or_table, *, include_nrows=True, flush=True):
    """Print CREATE TABLE for the given table and its number of rows."""
    with ENGINE.connect() as conn:
        result = conn.execute(select_table_sql(model_or_table))
        sql = result.scalar_one_or_none()

        if include_nrows:
            result = conn.execute(select_table_nrows(model_or_table))
            nrows = result.scalar_one_or_none()
        else:
            nrows = None

    print(sql, flush=flush)
    if nrows is not None:
        print(nrows, flush=flush)


def _get_table_name(model_or_table):
    if hasattr(model_or_table, '__tablename__'):
        return model_or_table.__tablename__
    elif hasattr(model_or_table, 'name'):
        return model_or_table.name
    return model_or_table


def select_table_sql(model_or_table):
    """Select CREATE_TABLE of the given table from sqlite_master."""
    select = (sa.select(sqlite_master.c.sql)
              .select_from(sqlite_master)
              .filter_by(type='table')
              .filter_by(name=sa.bindparam('table_name')))

    if model_or_table is not None:
        table_name = _get_table_name(model_or_table)
        select = select.params(table_name=table_name)
    return select


def select_table_nrows(model_or_table, *, label='n_rows'):
    """Select the number of rows for the given table."""
    table_name = _get_table_name(model_or_table)
    return (sa.select(sa.func.count().label(label))
           .select_from(sa.table(table_name)))


def select_tables_nrows(*, table_label='table_name', nrows_label='n_rows'):
    """Select table name and number of rows for all tables in sqlite_master."""
    with ENGINE.connect() as conn:  # requires dynamic query creation
        result = conn.execute(select_tables())
        tables = result.all()

    def iterselects(tables):
        for t, in tables:
            name = sa.literal(t)
            n = sa.select(sa.func.count().label('n')).select_from(sa.table(t))
            yield sa.select(name.label(table_label), n.label(nrows_label))

    return sa.union_all(*iterselects(tables))


def select_tables():
    """Select all table names from sqlite_master."""
    return (sa.select(sqlite_master.c.name)
            .select_from(sqlite_master)
            .filter_by(type='table')
            .where(~sqlite_master.c.name.like('sqlite_%'))
            .order_by('name'))


def select_views():
    return (sa.select(sqlite_master.c.name)
            .select_from(sqlite_master)
            .filter_by(type='view')
            .where(~sqlite_master.c.name.like('sqlite_%'))
            .order_by('name'))
