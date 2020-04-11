# sqlite_master.py - read sqlite3 sqlite_master tables

import sqlalchemy as sa

from . import ENGINE

__all__ = ['print_table_sql',
           'select_table_sql', 'select_table_nrows',
           'select_tables_nrows', 'select_tables']


def get_table_name(model_or_table):
    if hasattr(model_or_table, '__tablename__'):
        return model_or_table.__tablename__
    elif hasattr(model_or_table, 'name'):
        return model_or_table.name
    return model_or_table


sqlite_master = sa.table('sqlite_master',  # https://www.sqlite.org/faq.html#q7
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


def print_table_sql(model_or_table, *, include_nrows=True, bind=ENGINE):
    """Print CREATE TABLE for the given table and its number of rows."""
    print(select_table_sql(model_or_table, bind=bind).scalar())
    if include_nrows:
        print(select_table_nrows(model_or_table, bind=bind).scalar())


def select_table_sql(model_or_table, *, bind=ENGINE):
    """Select CREATE_TABLE of the given table from sqlite_master."""
    result = sa.select([sqlite_master.c.sql], bind=bind)\
             .where(sqlite_master.c.type == 'table')\
             .where(sqlite_master.c.name == sa.bindparam('table_name'))

    if model_or_table is not None:
        table_name = get_table_name(model_or_table)
        result = result.params(table_name=table_name)
    return result


def select_table_nrows(model_or_table, *, label='n_rows', bind=ENGINE):
    """Select the number of rows for the given table."""
    table_name = get_table_name(model_or_table)
    table = sa.table(table_name)
    return sa.select([sa.func.count().label(label)], bind=bind).select_from(table)


def select_tables_nrows(*, table_label='table_name', nrows_label='n_rows', bind=ENGINE):
    """Select table name and number of rows for all tables in sqlite_master."""
    tables = select_tables().execute()  # requires dynamic query creation

    def iterselects(tables):  
        for t, in tables:
            yield sa.select([sa.literal(t).label(table_label),
                             sa.select([sa.func.count().label('n')])\
                             .select_from(sa.table(t)).label(nrows_label)])

    return sa.union_all(*iterselects(tables), bind=bind)


def select_tables(*, bind=ENGINE):
    """Select all table names from sqlite_master."""
    return sa.select([sqlite_master.c.name], bind=bind)\
           .where(sqlite_master.c.type == 'table')\
           .where(~sqlite_master.c.name.like('sqlite_%'))\
           .order_by('name')
