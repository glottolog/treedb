# _basics.py - package-level globals

from sqlalchemy.orm import (registry as _registry,
                            sessionmaker as _sessionmaker)

from . import proxies as _proxies

FUTURE = True

ENGINE = _proxies.SQLiteEngineProxy(future=FUTURE)

ROOT = _proxies.PathProxy()

REGISTRY = _registry()

SESSION = _sessionmaker(bind=ENGINE, future=FUTURE)

__all__ = ['ENGINE', 'ROOT', 'REGISTRY', 'SESSION']
