# backend_views - limited sqlalchemy support for views

"""From https://github.com/sqlalchemy/sqlalchemy/wiki/Views"""

import logging

import sqlalchemy as sa
import sqlalchemy.ext.compiler

__all__ = ['view']


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
    log.debug('CREATE VIEW %r', element.name)
    select = compiler.sql_compiler.process(element.selectable,
                                           literal_binds=True)
    return f'CREATE VIEW {element.name} AS {select}'


@sa.ext.compiler.compiles(DropView)
def compile_drop_view(element, compiler, **kwargs):
    log.debug('DROP VIEW %r', element.name)
    return f'DROP VIEW {element.name}'


def view(name, selectable, *, metadata):
    t = sa.table(name)

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, selectable).execute_at('after-create', metadata)

    DropView(name).execute_at('before-drop', metadata)

    return t
