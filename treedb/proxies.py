# proxies.py - proxies for global default variables that can be updated

import sqlalchemy as sa

from . import tools as _tools

__all__ = ['SqliteEngineProxy']

UNSPECIFIED = object()


class EngineProxy(sa.engine.Engine):

    def __init__(self, engine=None):
        self.engine = engine

    def __getattr__(self, name):
        if name in ('_file', '_engine'):
            return super(EngineProxy, self).__getattr__(name)

        return getattr(self._engine, name)

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, engine):
        if engine is not None:
            assert engine.url.drivername == 'sqlite'

        if getattr(self, '_engine', None) is not None:
            self._engine.dispose()

        self._engine = engine

    def set_url(self, url, **kwargs):
        if url is None:
            url = 'sqlite://'
        self.engine = sa.create_engine(url, **kwargs)

    def __repr__(self):
        url = None
        if self._engine is not None:
            url = str(self.engine.url)

        return '<%s.%s url=%r>' % (self.__module__, self.__class__.__name__, url)


class SqliteEngineProxy(EngineProxy):

    _memory_path = None

    @property
    def file(self):
        if self.engine is None:
            return None
        file = self.engine.url.database
        if file is None:
            return None
        return _tools.path_from_filename(file)

    @file.setter
    def file(self, filename, resolve=True, memory_filename=UNSPECIFIED):
        if filename is None:
            url = None
            if memory_filename is not UNSPECIFIED:
                self._memory_path = _tools.path_from_filename(memory_filename)
        else:
            path = _tools.path_from_filename(filename)
            if resolve:
                path = path.resolve(strict=False)
            url = 'sqlite:///%s' % path

        self.set_url(url)

    def __repr__(self):
        if self.engine is None or self.file is None:
            return super(SqliteEngineProxy, self).__repr__()

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
