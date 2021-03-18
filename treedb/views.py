# views.py - literally serialized sqlalchemy queries for sqlite3 db

import functools
import importlib
import logging

__all__ = ['register_view',
           'create_all_views']

VIEW_REGISTRY = {}


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

    from .backend import views as _views

    module = importlib.import_module(__name__)

    for name, func in VIEW_REGISTRY.items():
        table = _views.view(name, selectable=func(), clear=clear)

        present = hasattr(module, name)
        setattr(module, name, table)

        if not present:
            module.__all__.append(name)
