# tools.py - sqlite3 database tools

import logging

import sqlalchemy as sa
import sqlalchemy.ext.compiler

from .. import ENGINE, REGISTRY

from . import sqlparse

__all__ = ['print_query_sql', 'get_query_sql',
           'expression_compile',
           'print_schema']


log = logging.getLogger(__name__)


def print_query_sql(query=None, *, literal_binds=True, pretty=True, flush=True):
    """Print the literal SQL for the given query."""
    sql = get_query_sql(query, literal_binds=literal_binds, pretty=pretty)
    print(sql, flush=flush)


def get_query_sql(query=None, *, literal_binds=True, pretty=False):
    """Return the literal SQL for the given query."""
    if query is None:
        from .. import queries

        query = queries.get_query()

    compiled = expression_compile(query, literal_binds=literal_binds)
    result = compiled.string

    if pretty and sqlparse is not None:
        result = sqlparse.format(result, reindent=True)
    return result


def expression_compile(expression, *, literal_binds=True):
    """Return literal compiled expression."""
    return expression.compile(compile_kwargs={'literal_binds': literal_binds})


@sa.ext.compiler.compiles(sa.schema.CreateTable)
def compile(element, compiler, **kwargs):
    """Append sqlite3 WITHOUT_ROWID to CREATE_TABLE if configured.

    From https://gist.github.com/chaoflow/3a6dc9d42a90c38870b8d4033b58a4d1
    """
    text = compiler.visit_create_table(element, **kwargs)
    if element.element.info.get('without_rowid'):
        text = text.rstrip() + ' WITHOUT ROWID'
    return text


def print_schema(metadata=REGISTRY.metadata, *, engine=ENGINE):
    """Print the SQL from metadata.create_all() without executing."""
    def print_sql(sql):
        print(sql.compile(dialect=engine.dialect))

    mock_engine = sa.create_mock_engine(engine.url, executor=print_sql)

    metadata.create_all(mock_engine, checkfirst=False)
