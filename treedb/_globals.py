# package-level globals

import datetime
import typing

from sqlalchemy.orm import (registry as _registry,
                            sessionmaker as _sessionmaker)

from . import _proxies

CONFIG = 'treedb.ini'

DEFAULT_ROOT = './glottolog/'

SQLALCHEMY_FUTURE = True

ENGINE = _proxies.SQLiteEngineProxy(future=SQLALCHEMY_FUTURE)

ROOT = _proxies.PathProxy()

REGISTRY = _registry()

SESSION = _sessionmaker(bind=ENGINE, future=SQLALCHEMY_FUTURE)

__all__ = ['CONFIG', 'DEFAULT_ROOT',
           'ENGINE', 'ROOT',
           'REGISTRY',
           'SESSION']


PathType = typing.Tuple[str, ...]


RecordValueType = typing.Union[str, typing.List[str]]


RecordType = typing.Mapping[str, typing.Mapping[str, RecordValueType]]


RecordItem = typing.Tuple[PathType, RecordType]


ValueType = typing.Union[str,
                         int, float,
                         bool,
                         None,
                         datetime.datetime,
                         typing.List[str]]


MatchType = typing.Mapping[str, ValueType]


MatchList = typing.List[MatchType]


MatchListMapping = typing.Mapping[str, MatchList]


LanguoidValueType = typing.Union[ValueType,
                                 MatchType,
                                 MatchList,
                                 MatchListMapping,
                                 typing.Mapping[str, MatchListMapping]]


LanguoidType = typing.Mapping[str, LanguoidValueType]


LanguoidItem = typing.Tuple[PathType, LanguoidType]
