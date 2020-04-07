# views.py - sqlalchemy queries for sqlite3 db

from . import (backend as _backend,
               queries as _queries)

__all__ = ['stats',
           'path_json']


@_backend.view('stats')
def stats():
    return _queries.get_stats_query()


@_backend.view('path_json')
def path_json():
    return _queries.get_json_query(load_json=False)
