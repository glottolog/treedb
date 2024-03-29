"""Package-level globals."""

import datetime
import typing

from sqlalchemy.orm import (registry as _registry,
                            sessionmaker as _sessionmaker)

from . import _proxies

__all__ = ['DEFAULT_FILESTEM', 'CONFIG', 'MEMORY_TAG', 'DEFAULT_ROOT',
           'PATH_LABEL', 'LANGUOID_LABEL', 'LANGUOID_ORDER',
           'DEFAULT_HASH', 'FILE_PATH_SEP',
           'ENGINE', 'ROOT',
           'REGISTRY',
           'SESSION',
           'RecordItem',
           'LanguoidItem']

DEFAULT_FILESTEM = 'treedb'

CONFIG = 'treedb.ini'

LANGUOID_FILE_BASENAME = 'md.ini'

MEMORY_TAG = '-memory'

DEFAULT_ROOT = './glottolog/'

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


assert PATH_LABEL < LANGUOID_LABEL


PathType = typing.Tuple[str, ...]


RecordValueType = typing.Union[str, typing.List[str]]


RecordType = typing.Mapping[str, typing.Mapping[str, RecordValueType]]


def filepath_tuple(file_path: str, /, *,
                   sep=FILE_PATH_SEP) -> typing.Tuple[str]:
    path_parts = file_path.split(sep)
    return tuple(path_parts)


class RecordItem(typing.NamedTuple):
    """Pair of path and record.

    >>> RecordItem.from_filepath_record('spam/eggs', {'core': {'id': 'abin1243'}})
    RecordItem(path=('spam', 'eggs'), record={'core': {'id': 'abin1243'}})
    """

    path: PathType

    record: RecordType

    @classmethod
    def from_filepath_record(cls, file_path: str, languoid, /):
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
    """Pair of path and languoid.

    >>> LanguoidItem.from_filepath_languoid('spam/eggs', {'id': 'abin1243'})
    LanguoidItem(path=('spam', 'eggs'), languoid={'id': 'abin1243'})
    """

    path: PathType

    languoid: LanguoidType

    @classmethod
    def from_filepath_languoid(cls, file_path: str, languoid, /):
        return cls(filepath_tuple(file_path), languoid)
