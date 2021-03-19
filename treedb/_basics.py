# _basics.py - package-level globals

from sqlalchemy.orm import (registry as _registry,
                            sessionmaker as _sessionmaker)

from . import _proxies

CONFIG = 'treedb.ini'

DEFAULT_ROOT = './glottolog/'

FUTURE = True

ENGINE = _proxies.SQLiteEngineProxy(future=FUTURE)

ROOT = _proxies.PathProxy()

REGISTRY = _registry()

SESSION = _sessionmaker(bind=ENGINE, future=FUTURE)

__all__ = ['CONFIG', 'DEFAULT_ROOT',
           'ENGINE', 'ROOT',
           'REGISTRY',
           'SESSION']
