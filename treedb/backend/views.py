# views.py - limited sqlalchemy support for views

"""Based on https://github.com/sqlalchemy/sqlalchemy/wiki/Views"""

import functools
import logging

import sqlalchemy as sa
import sqlalchemy.ext.compiler

from .. import REGISTRY

__all__ = ['register_view',
           'create_all_views',
           'view',
           'make_table']

VIEW_REGISTRY = {}

DDL = {}


log = logging.getLogger(__name__)


def register_view(name, **kwargs):
    log.debug('register_view(%r)', name)
    assert name not in VIEW_REGISTRY

    def decorator(func):
        VIEW_REGISTRY[name] = functools.partial(func, **kwargs)
        return func

    return decorator


def create_all_views(*, clear=False):
    log.debug('run create_view() for %d views in VIEW_REGISTRY', len(VIEW_REGISTRY))

    ns = globals()
    for name, func in VIEW_REGISTRY.items():
        present = name in ns

        ns[name] = view(name, selectable=func(), clear=clear)
        if not present:
            __all__.append(name)


def view(name, selectable, *, clear=False):
    """Register a CREATE and DROP VIEW DDL for the given selectable."""
    log.debug('view(%r, clear=%r)', name, clear)

    if clear:
        DDL[name] = None, None
        return None

    DDL[name] = (CreateView(name, selectable),
                 DropView(name))

    return make_table(selectable, name=name)


def make_table(selectable, *, name='view_table'):
    table = sa.table(name)
    for c in selectable.alias().c:
        _, col = c._make_proxy(table)
        table.append_column(col)
    return table


@sa.event.listens_for(REGISTRY.metadata, 'after_create')
def after_create(target, bind, **kwargs):
    for name, (create_view, _) in DDL.items():
        if create_view is not None:
            log.debug('CREATE VIEW %r', name)
            create_view(target, bind)


@sa.event.listens_for(REGISTRY.metadata, 'before_drop')
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
