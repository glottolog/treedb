# languoids.py - load ../../languoids/tree/**/md.ini into dicts

import datetime
import functools
import json
import logging
import operator
import warnings

import csv23

from . import (tools as _tools,
               queries as _queries,
               languoids as _languoids)

from . import ENGINE

__all__ = ['iterlanguoids',
           'write_json_csv']


log = logging.getLogger(__name__)


def iterlanguoids(bind=ENGINE, *, ordered='id',
                  progress_after=_tools.PROGRESS_AFTER):
    log.info('select languoids from json query')

    query = _queries.get_json_query(bind=bind,
                                    ordered=ordered)

    json_datetime = datetime.datetime.fromisoformat

    n = 0
    for n, (s,) in enumerate(query.execute(), 1):
        path, item = json.loads(s)

        endangerment = item['endangerment']
        if endangerment is not None:
            endangerment['date'] = json_datetime(endangerment['date'])

        yield tuple(path), item
        if not (n % progress_after):
            log.info('%s languoids fetched', f'{n:_d}')

    log.info('%s languoids total', f'{n:_d}')


def write_json_csv(bind_or_root=ENGINE, filename=None, *,
                   from_raw=False, ordered=True, sort_keys=True,
                   dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    if filename is None:
        suffix = '.languoids-json.csv'
        try:
            path = bind_or_root.file_with_suffix(suffix)
        except AttributeError:
            path = _tools.path_from_filename(bind_or_root).with_suffix(suffix)
        filename = path.name
    else:
        filename = _tools.path_from_filename(filename)

    log.info('write json csv: %r', filename)
    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'delete peresent file {path!r}')
        path.unlink()

    json_dumps = functools.partial(json.dumps,
                                   # json-serialize datetime.datetime
                                   default=operator.methodcaller('isoformat'),
                                   sort_keys=sort_keys)

    rows = _languoids.iterlanguoids(bind_or_root,
                                    from_raw=from_raw,
                                    ordered=ordered)

    rows = (('/'.join(path_tuple), json_dumps(l)) for path_tuple, l in rows)

    header = ['path', 'json']
    log.info('header: %r', header)

    return csv23.write_csv(filename, rows, header=header,
                            dialect=dialect, encoding=encoding)
