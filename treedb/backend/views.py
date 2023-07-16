"""Limited ``sqlalchemy`` support for views based on https://github.com/sqlalchemy/sqlalchemy/wiki/Views."""

import functools
import logging
import types

import sqlalchemy as sa
import sqlalchemy.ext.compiler

from .. import _globals

__all__ = ['register_view',
           'create_all_views',
           'view',
           'make_table']

REGISTERED = {}

DDL = {}

TABLES = types.SimpleNamespace()


log = logging.getLogger(__name__)


def register_view(name, **kwargs):
    log.debug('register_view(%r)', name)
    assert name not in REGISTERED

    def decorator(func):
        REGISTERED[name] = functools.partial(func, **kwargs)
        return func

    return decorator


def create_all_views(*, clear: bool = False):
    log.debug('run create_view() for %d views in REGISTERED', len(REGISTERED))
    for name, func in REGISTERED.items():
        table = view(name, selectable=func(), clear=clear)
        setattr(TABLES, name, table)


def view(name, selectable, *, clear: bool = False):
    """Register a CREATE and DROP VIEW DDL for the given selectable."""
    log.debug('view(%r, clear=%r)', name, clear)

    if clear:
        DDL[name] = None, None
        return None

    DDL[name] = (CreateView(name, selectable),
                 DropView(name))

    return make_table(selectable, name=name)


def make_table(selectable, /, *, name: str = 'view_table'):
    table = sa.table(name)
    for c in selectable.alias().c:
        _, col = c._make_proxy(table)
        table.append_column(col)
    return table


@sa.event.listens_for(_globals.REGISTRY.metadata, 'after_create')
def after_create(target, bind, **kwargs):
    for name, (create_view, _) in DDL.items():
        if create_view is not None:
            log.debug('CREATE VIEW %r', name)
            create_view(target, bind)


@sa.event.listens_for(_globals.REGISTRY.metadata, 'before_drop')
def before_drop(target, bind, **kwargs):
    for name, (_, drop_view) in DDL.items():
        if drop_view is not None:
            log.debug('DROP VIEW %r', name)
            drop_view(target, bind)


class CreateView(sa.schema.DDLElement):

    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(sa.schema.DDLElement):

    def __init__(self, name):
        self.name = name


@sa.ext.compiler.compiles(CreateView)
def compile_create_view(element, compiler, **kwargs):
    select = compiler.sql_compiler.process(element.selectable,
                                           literal_binds=True)
    return f'\nCREATE VIEW {element.name} AS {select}\n'


@sa.ext.compiler.compiles(DropView)
def compile_drop_view(element, compiler, **kwargs):
    return f'\nDROP VIEW {element.name}\n'
