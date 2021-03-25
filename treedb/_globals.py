# package-level globals

import datetime
import typing

from sqlalchemy.orm import (registry as _registry,
                            sessionmaker as _sessionmaker)

from . import _proxies

CONFIG = 'treedb.ini'

DEFAULT_ROOT = './glottolog/'

DEFAULT_ENGINE = 'treedb.sqlite3'

PATH_LABEL = '__path__'

LANGUOID_LABEL = 'languoid'

LANGUOID_ORDER = 'path'

DEFAULT_HASH = 'sha256'

FILE_PATH_SEP = '/'

_SQLALCHEMY_FUTURE = True

ENGINE = _proxies.SQLiteEngineProxy(future=_SQLALCHEMY_FUTURE)

ROOT = _proxies.PathProxy()

REGISTRY = _registry()

SESSION = _sessionmaker(bind=ENGINE, future=_SQLALCHEMY_FUTURE)

__all__ = ['CONFIG', 'DEFAULT_ROOT', 'DEFAULT_ENGINE',
           'PATH_LABEL', 'LANGUOID_LABEL', 'LANGUOID_ORDER',
           'DEFAULT_HASH', 'FILE_PATH_SEP',
           'ENGINE', 'ROOT',
           'REGISTRY',
           'SESSION',
           'RecordItem',
           'LanguoidItem']


assert PATH_LABEL < LANGUOID_LABEL


PathType = typing.Tuple[str, ...]


RecordValueType = typing.Union[str, typing.List[str]]


RecordType = typing.Mapping[str, typing.Mapping[str, RecordValueType]]


def filepath_tuple(file_path: str,
                   *, sep=FILE_PATH_SEP) -> typing.Tuple[str]:
    path_parts = file_path.split(sep)
    return tuple(path_parts)


class RecordItem(typing.NamedTuple):

    path: PathType

    record: RecordType

    @classmethod
    def from_filepath_record(cls, file_path: str, languoid):
        return cls(filepath_tuple(file_path), languoid)


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


class LanguoidItem(typing.NamedTuple):

    path: PathType

    languoid: LanguoidType

    @classmethod
    def from_filepath_languoid(cls, file_path: str, languoid):
        return cls(filepath_tuple(file_path), languoid)
