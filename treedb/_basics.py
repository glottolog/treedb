# _basics.py - package-level globals

from sqlalchemy.orm import sessionmaker as _sessionmaker

from . import proxies as _proxies

FUTURE = False

ENGINE = _proxies.SQLiteEngineProxy(future=FUTURE)

ROOT = _proxies.PathProxy()

SESSION = _sessionmaker(bind=ENGINE, future=FUTURE)

__all__ = ['ENGINE', 'ROOT', 'SESSION']
