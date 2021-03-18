# _basics.py - sqlite3 database sub-module level globals

import contextlib
import logging
import sqlite3

import csv23

import sqlalchemy as sa

from .. import ENGINE

from .. import proxies as _proxies
from .. import tools as _tools

from . import sqlparse

__all__ = ['set_engine',
           'scalar',
           'iterrows',
           'connect']


log = logging.getLogger(__name__)


def set_engine(filename, *, resolve=False, require=False, title=None):
    """Return new sqlite3 engine and set it as default engine for treedb."""
    log.info('set_engine: %r', filename)

    if isinstance(filename, sa.engine.Engine):
        engine = filename
        if isinstance(filename, _proxies.EngineProxy):
            engine = engine.engine

        ENGINE.engine = engine
        return ENGINE

    if filename is None:
        if title is not None:
            ENGINE._memory_path = _tools.path_from_filename(f'{title}-memory',
                                                            expanduser=False)
    else:
        filename = _tools.path_from_filename(filename)
        if resolve:
            filename = filename.resolve(strict=False)

        if require and not filename.exists():
            log.error('required engine file not found: %r', filename)
            raise RuntimeError(f'engine file does not exist: {filename!r}')

    ENGINE.file = filename
    log.info('sqlite version: %s', ENGINE.dialect.dbapi.sqlite_version)
    log.info('sqlalchemy version: %s', sa.__version__)
    log.info('csv23 version: %s', csv23.__version__)
    if sqlparse is not None:
        log.info('sqlparse version: %s', sqlparse.__version__)
    return ENGINE


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks."""
    if not isinstance(dbapi_conn, sqlite3.Connection):
        return

    log.debug('connect sqlalchemy.engine.Engine: enable foreign keys')

    with contextlib.closing(dbapi_conn.cursor()) as cursor:
        cursor.execute('PRAGMA foreign_keys = ON')

    log.debug('dbapi_conn: %r', dbapi_conn)


def scalar(statement, *args, bind=ENGINE, **kwargs):
    with connect(bind) as conn:
        return conn.scalar(statement, *args, **kwargs)


def iterrows(query, *, mappings=False, bind=ENGINE):
    with connect(bind) as conn:
        result = conn.execute(query)

        if mappings:
            result = result.mappings()

        yield from result


def connect(engine_or_conn=ENGINE):
    if isinstance(engine_or_conn, sa.engine.base.Connection):
        log.debug('nested connect (no-op): %r', engine_or_conn)
        return _tools.nullcontext(engine_or_conn)

    log.debug('engine connect: %r', engine_or_conn)
    conn = engine_or_conn.connect()
    log.debug('conn: %r', conn)
    return conn
