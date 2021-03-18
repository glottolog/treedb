# views.py - limited sqlalchemy support for views

"""Based on https://github.com/sqlalchemy/sqlalchemy/wiki/Views"""

import logging

import sqlalchemy as sa
import sqlalchemy.ext.compiler

from .. import REGISTRY

__all__ = ['view', 'make_table']

DDL = {}


log = logging.getLogger(__name__)


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


def view(name, selectable, *, clear=False):
    """Register a CREATE and DROP VIEW DDL for the given selectable."""
    log.debug('view(%r, clear=%r)', name, clear)

    if clear:
        DDL[name] = None, None
        return None

    DDL[name] = (CreateView(name, selectable), DropView(name))
    return make_table(selectable, name=name)


def make_table(selectable, *, name='view_table'):
    selectable = selectable.alias()
    table = sa.table(name)
    for c in selectable.c:
        c._make_proxy(table)
    return table
