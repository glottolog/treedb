# views.py - literally serialized sqlalchemy queries for sqlite3 db

import functools
import importlib
import logging

from . import backend as _backend

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


def create_all_views():
    log.debug('run create_view() for %d views in REGISTRY', len(REGISTRY))

    module = importlib.import_module(__name__)

    for name, func in REGISTRY.items():
        assert not hasattr(module, name)

        table = _backend.create_view(name, selectable=func())

        setattr(module, name, table)
        module.__all__.append(name)
