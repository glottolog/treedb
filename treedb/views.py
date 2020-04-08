# views.py - batteries-included queries for sqlite3 db

from . import (backend as _backend,
               queries as _queries)

__all__ = ['stats',
           'path_json']


def view(name):
    def decorator(func):
        return _backend.create_view(name, selectable=func())

    return decorator


@view('stats')
def stats():
    return _queries.get_stats_query()


@view('path_json')
def path_json():
    return _queries.get_json_query(load_json=False)
