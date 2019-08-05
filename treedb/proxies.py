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


class PathProxy(Proxy):

    def __init__(self, path=None):
        self.path = path

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

    def __fspath__(self):
        return self._delegate.__fspath__()

    def __str__(self):
        if self._delegate is None:
            raise RuntimeError('str() on empty path proxy')
        return str(self._delegate)

    def __repr__(self):
        path = self._delegate.as_posix() if self._delegate is not None else None
        return '<%s.%s path=%r>' % (self.__module__, self.__class__.__name__,
                                    path)


class EngineProxy(Proxy, sa.engine.Engine):

    def __init__(self, engine=None):
        self.engine = engine

    def set_url(self, url, **kwargs):
        if url is None:
            url = 'sqlite://'
        log.debug('create_engine %r', url)
        self.engine = sa.create_engine(url, **kwargs)

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

    def __repr__(self):
        url = str(self.url) if self._delegate is not None else None
        return '<%s.%s url=%r>' % (self.__module__, self.__class__.__name__, url)


class SQLiteEngineProxy(EngineProxy):

    _memory_path = None

    @property
    def file(self):
        if self.engine is None or self.engine.url.database is None:
            return None
        return _tools.path_from_filename(self.engine.url.database)

    @file.setter
    def file(self, filename):
        if filename is None:
            url = None
        else:
            url = 'sqlite:///%s' % filename
        self.set_url(url)

    def __repr__(self):
        if self.engine is None or self.file is None:
            return super(SQLiteEngineProxy, self).__repr__()

        parent = ''
        name = self.file.as_posix()
        if self.file.is_absolute():
            parent = ' parent=%r' % self.file.parent.name
            name = self.file.name

        return ('<%s.%s filename=%r' '%s'
                ' size=%r>' % (self.__module__, self.__class__.__name__,
                               name, parent, self.file_size()))

    def file_with_suffix(self, suffix):
        path = self.file if self.file is not None else self._memory_path
        return path.with_suffix(suffix)

    def file_exists(self):
        return self.file is not None and self.file.exists()

    def file_size(self):
        return self.file.stat().st_size if self.file_exists() else None

    def file_sha256(self):
        return _tools.sha256sum(self.file) if self.file_exists() else None
