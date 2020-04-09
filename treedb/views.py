# views.py - literally serialized sqlalchemy queries for sqlite3 db

import functools
import importlib
import logging

__all__ = ['register_view',
           'create_all_views']

REGISTRY = {}


log = logging.getLogger(__name__)


def register_view(name, **kwargs):
    log.debug('register_view(%r)', name)

    assert name not in REGISTRY

    def decorator(func):
        REGISTRY[name] = functools.partial(func, **kwargs)
        return func

    return decorator


def create_all_views(*, clear=False):
    log.debug('run create_view() for %d views in REGISTRY', len(REGISTRY))

    from . import backend_views

    module = importlib.import_module(__name__)

    for name, func in REGISTRY.items():
        table = backend_views.view(name, selectable=func(), clear=clear)

        present = hasattr(module, name)
        setattr(module, name, table)

        if not present:
            module.__all__.append(name)
