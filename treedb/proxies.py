# proxies.py - proxies for global default variables that can be updated

import sqlalchemy as sa

__all__ = ['EngineProxy']


class EngineProxy(sa.engine.Engine):

    def __init__(self, engine=None):
        self.engine = engine

    def __getattr__(self, name):
        return getattr(self._engine, name)

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, engine):
        if engine is not None:
            assert engine.url.drivername == 'sqlite'
        self._engine = engine

    def set_url(self, url, **kwargs):
        if url is None:
            url = 'sqlite://'
        self.engine = sa.create_engine(url, **kwargs)

    def set_file(self, filename, **kwargs):
        if filename is None:
            url = None
        else:
            url = 'sqlite:///%s' % filename
        self.set_url(url, **kwargs)

    def __repr__(self):
        tmpl = '<%s.%s at %#x>'
        args = self.__module__, self.__class__.__name__
        if self._engine is not None:
            tmpl = '<%s.%s url=%r at %#x>'
            args += str(self._engine.url),
        args += id(self),
        return  tmpl % args
