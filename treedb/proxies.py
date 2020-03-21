# proxies.py - proxies for global default variables that can be updated

import logging

import sqlalchemy as sa

from . import tools as _tools

__all__ = ['PathProxy', 'SQLiteEngineProxy']


log = logging.getLogger(__name__)


class Proxy(object):

    _delegate = None

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def __repr__(self):
        return f'<{self.__module__}.{self.__class__.__name__}>'


class PathProxy(Proxy):

    def __init__(self, path=None):
        self.path = path

    def __fspath__(self):
        return self._delegate.__fspath__()

    def __str__(self):
        if self._delegate is None:
            raise RuntimeError('str() on empty path proxy')
        return str(self._delegate)

    @property
    def path(self):
        return self._delegate

    @path.setter
    def path(self, path):
        if path is not None:
            path = _tools.path_from_filename(path)
        if self._delegate is None:
            log.debug('set root path %r', path)
        else:
            log.debug('replace root path %r with %r', self._delegate, path)
        self._delegate = path

    def __repr__(self):
        if self.path is None:
            return super().__repr__()
        return (f'<{self.__module__}.{self.__class__.__name__}'
                f' path={self.path.as_posix()!r}'
                f' inode={self.inode()!r}>')

    def inode(self):
        if self.path is None or not self.path.exists():
            return None
        return self.path.stat().st_ino


class EngineProxy(Proxy, sa.engine.Engine):

    def __init__(self, engine=None):
        self.engine = engine

    @property
    def engine(self):
        return self._delegate

    @engine.setter
    def engine(self, engine):
        if engine is not None:
            assert engine.url.drivername == 'sqlite'

        if self._delegate is not None:
            log.debug('dispose engine %r', self._delegate)
            self._delegate.dispose()

        self._delegate = engine

    @property
    def url(self):
        return self.engine.url if self.engine is not None else None

    @url.setter
    def url(self, url):
        log.debug('create_engine %r', url)
        self.engine = sa.create_engine(url)

    def __repr__(self):
        if self.engine is None:
            return super().__repr__()
        return (f'{self.__module__}.{self.__class__.__name__}'
                f' url={str(self.url)!r}>')


class SQLiteEngineProxy(EngineProxy):

    _memory_path = None

    @property
    def file(self):
        if self.url is None or self.url.database is None:
            return None
        return _tools.path_from_filename(self.url.database)

    @file.setter
    def file(self, filename):
        url = 'sqlite://'
        if filename is not None:
            url += f'/{filename}'
        self.url = url

    def __repr__(self):
        if self.file is None:
            return super().__repr__()

        parent = ''
        name = self.file.as_posix()
        if self.file.is_absolute():
            parent = f' parent={self.file.parent.name!r}'
            name = self.file.name

        return (f'<{self.__module__}.{self.__class__.__name__}'
                f' filename={name!r}{parent}'
                f' size={self.file_size()!r}>')

    def file_with_suffix(self, suffix):
        path = self.file if self.file is not None else self._memory_path
        return path.with_suffix(suffix)

    def file_exists(self):
        return self.file is not None and self.file.exists()

    def file_size(self):
        return self.file.stat().st_size if self.file_exists() else None

    def file_sha256(self):
        return _tools.sha256sum(self.file) if self.file_exists() else None
