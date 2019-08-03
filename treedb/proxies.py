# proxies.py - proxies for global default variables that can be updated

import sqlalchemy as sa

from . import tools as _tools

__all__ = ['SqliteEngineProxy']


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

    @property
    def file(self):
        if self.engine is None:
            return None
        file = self.engine.url.database
        if file is not None:
            file = _tools.path_from_filename(file)
        return file

    @file.setter
    def file(self, filename, resolve=True):
        if filename is None:
            url = None
        else:
            path = _tools.path_from_filename(filename)
            if resolve:
                path = path.resolve().absolute()
            url = 'sqlite:///%s' % path

        self.set_url(url)

    def __repr__(self):
        if self.engine is None or self.file is None:
            return super(SqliteEngineProxy, self).__repr__()

        size = None
        if self.file.exists():
            size = self.file.stat().st_size
        parent = ''
        name = self.file.as_posix()
        if self.file.is_absolute():
            parent = ' parent=%r' % self.file.parent.name
            name = self.file.name

        return ('<%s.%s filename=%r' '%s'
                ' size=%r>' % (self.__module__, self.__class__.__name__,
                               name, parent, size))
