# _basics.py - sqlite3 database sub-module level globals

import contextlib
import logging
import sqlite3
import typing

import csv23

import sqlalchemy as sa
import sqlalchemy.ext.compiler

from .._globals import ENGINE

from .. import _compat
from .. import _proxies
from .. import _tools
from .. import logging_

from . import sqlparse

__all__ = ['print_versions',
           'set_engine',
           'connect',
           'scalar',
           'iterrows']


log = logging.getLogger(__name__)


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks."""
    if not isinstance(dbapi_conn, sqlite3.Connection):  # pragma: no cover
        return

    log.debug('connect sqlalchemy.engine.Engine: enable foreign keys')

    with contextlib.closing(dbapi_conn.cursor()) as dbapi_cursor:
        dbapi_cursor.execute('PRAGMA foreign_keys = ON')

    log.debug('dbapi_conn: %r', dbapi_conn)


@sa.ext.compiler.compiles(sa.schema.CreateTable)
def compile(element, compiler, **kwargs):
    """Append sqlite3 WITHOUT_ROWID to CREATE_TABLE if configured.

    From https://gist.github.com/chaoflow/3a6dc9d42a90c38870b8d4033b58a4d1
    """
    text = compiler.visit_create_table(element, **kwargs)
    if element.element.info.get('without_rowid'):
        text = text.rstrip() + ' WITHOUT ROWID'
    return text


def print_versions(*, engine=ENGINE, file=None):
    logging_.log_version(also_print=True, print_file=file)
    log_versions(also_print=True, print_file=file,
                 engine=engine)


def set_engine(filename, *,
               resolve: bool = False,
               require: bool = False,
               title: typing.Optional[str] = None):
    """Return new sqlite3 engine and set it as default engine for treedb."""
    log.info('set_engine: %r', filename)

    if isinstance(filename, sa.engine.Engine):
        engine = filename
        if isinstance(filename, _proxies.EngineProxy):
            engine = engine.engine

        ENGINE.engine = engine
        return ENGINE

    if filename is None:
        if title is None:
            raise TypeError(f'filename=None requires title, given: {title!r}')
        ENGINE.memory_write_path = _tools.path_from_filename(f'{title}-memory',
                                                             expanduser=False)
    else:
        del title

        filename = _tools.path_from_filename(filename)
        if resolve:
            filename = filename.resolve(strict=False)

        if require and not filename.exists():
            log.error('required engine file not found: %r', filename)
            raise RuntimeError(f'engine file does not exist: {filename!r}')

    ENGINE.file = filename
    log_versions(engine=ENGINE)
    return ENGINE


def log_versions(*, also_print=False, print_file=None,
                 engine=ENGINE):
    log.info('sqlalchemy version: %s', sa.__version__)
    log.info('sqlite version: %s', engine.dialect.dbapi.sqlite_version)
    log.info('csv23 version: %s', csv23.__version__)
    if also_print or print_file is not None:
        print(f'sqlalchemy version: {sa.__version__}',
              file=print_file)
        print(f'sqlite_version: {engine.dialect.dbapi.sqlite_version}',
              file=print_file)
        print(f'csv23 version: {csv23.__version__}',
              file=print_file)
    if sqlparse is not None:  # pragma: no cover
        log.info('sqlparse version: %s', sqlparse.__version__)
        if also_print or print_file is not None:
            print(f'sqlparse version: {sqlparse.__version__}',
                  file=print_file)


def connect(*, bind=ENGINE):
    if isinstance(bind, sa.engine.base.Connection):
        log.debug('nested connect (no-op): %r', bind)
        return _compat.nullcontext(bind)

    log.debug('engine connect: %r', bind)
    conn = bind.connect()
    log.debug('conn: %r', conn)
    return conn


def scalar(statement, *args, bind=ENGINE, **kwargs):
    with connect(bind=bind) as conn:
        return conn.scalar(statement, *args, **kwargs)


def iterrows(query, *, mappings=False, bind=ENGINE):
    with connect(bind=bind) as conn:
        result = conn.execute(query)

        if mappings:
            result = result.mappings()

        yield from result


def expression_compile(expression, *, literal_binds=True):
    """Return literal compiled expression."""
    return expression.compile(compile_kwargs={'literal_binds': literal_binds})
