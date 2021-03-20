# _basics.py - package-level globals

import typing

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


PathType = typing.Tuple[str, ...]


RecordValueType = typing.Union[str, typing.List[str]]


RecordType = typing.Mapping[str, typing.Mapping[str, RecordValueType]]


RecordItem = typing.Tuple[PathType, RecordType]


LanguoidValueType = typing.Union[str, int, float, bool, None,
                                 typing.List[str],
                                 typing.Mapping[str, typing.Any],
                                 typing.List[typing.Mapping[str, typing.Any]]]


LanguoidType = typing.Mapping[str, LanguoidValueType]


LanguoidItem = typing.Tuple[PathType, LanguoidType]
