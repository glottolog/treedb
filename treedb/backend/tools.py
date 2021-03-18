# tools.py - sqlite3 database tools

import logging

import sqlalchemy as sa

from .. import ENGINE, REGISTRY

from .. import backend as _backend

__all__ = ['print_schema',
           'print_query_sql',
           'get_query_sql']


log = logging.getLogger(__name__)


def print_schema(metadata=REGISTRY.metadata, *, engine=ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql):
        print(sql.compile(dialect=engine.dialect))

    mock_engine = sa.create_mock_engine(engine.url, executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)


def print_query_sql(query=None, *, literal_binds=True, pretty=True, flush=True):
    """Print the literal SQL for the given query."""
    sql = get_query_sql(query, literal_binds=literal_binds, pretty=pretty)
    print(sql, flush=flush)


def get_query_sql(query=None, *, literal_binds=True, pretty=False):
    """Return the literal SQL for the given query."""
    if query is None:
        from .. import queries

        query = queries.get_query()

    compiled = _backend.expression_compile(query, literal_binds=literal_binds)
    result = compiled.string

    if pretty and _backend.sqlparse is not None:
        result = _backend.sqlparse.format(result, reindent=True)
    return result
