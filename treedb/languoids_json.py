# languoids_json.py - load languoids from tables and write json csv

import datetime
import functools
import json
import logging
import operator
import warnings

import csv23

from . import (_compat,
               tools as _tools,
               queries as _queries,
               languoids as _languoids)

from . import ENGINE, ROOT

__all__ = ['iterlanguoids',
           'write_json_csv',
           'checksum']


log = logging.getLogger(__name__)


def iterlanguoids(bind=ENGINE, *, ordered='id',
                  progress_after=_tools.PROGRESS_AFTER):
    log.info('select languoids from json query')
    log.info('ordered: %r', ordered)

    query = _queries.get_json_query(bind=bind,
                                    ordered=ordered,
                                    load_json=True)

    json_datetime = _compat.datetime_fromisoformat

    n = 0
    for n, (path, item) in enumerate(query.execute(), 1):
        endangerment = item['endangerment']
        if endangerment is not None:
            endangerment['date'] = json_datetime(endangerment['date'])

        yield tuple(path.split('/')), item

        if not (n % progress_after):
            log.info('%s languoids fetched', f'{n:_d}')

    log.info('%s languoids total', f'{n:_d}')


def write_json_csv(*, source='tables', filename=None,
                   file_order=False, file_means_path=True,
                   sort_keys=True,
                   dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""

    kwargs = _json_kwargs(source=source,
                          file_order=file_order,
                          file_means_path=file_means_path)

    return _write_json_csv(filename=filename, sort_keys=sort_keys,
                           dialect=csv23.DIALECT, encoding=csv23.ENCODING,
                           **kwargs)


def _json_kwargs(*, source, file_order, file_means_path):
    try:
        kwargs = {'tables': {'bind_or_root': ENGINE,
                             'from_raw': False},
                  'raw': {'bind_or_root': ENGINE,
                          'from_raw': True},
                  'files': {'bind_or_root': ROOT,
                            'from_raw': False}}[source]
    except KeyError:
        raise ValueError(f'unknown checksum source: {source!r}'
                         f' (possible values: {list(kwargs)})')

    by_file = 'path' if file_means_path else 'file'
    kwargs['ordered'] = by_file if file_order else 'id'
    return kwargs


def _write_json_csv(bind_or_root=ENGINE, filename=None, *,
                   from_raw=False, ordered=True, sort_keys=True,
                   dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    if filename is None:
        suffix = '.languoids-json.csv.gz'
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

    header = ['path', 'json']
    log.info('header: %r', header)

    rows = _json_rows(bind_or_root,
                      from_raw=from_raw,
                      ordered=ordered,
                      sort_keys=sort_keys)

    return csv23.write_csv(filename, rows, header=header,
                           dialect=dialect, encoding=encoding,
                           autocompress=True)


def _json_rows(bind_or_root=ENGINE, *,
               from_raw=False, ordered=True, sort_keys=True):
    json_dumps = functools.partial(json.dumps,
                                   # json-serialize datetime.datetime
                                   default=operator.methodcaller('isoformat'),
                                   sort_keys=sort_keys)

    rows = _languoids.iterlanguoids(bind_or_root,
                                    from_raw=from_raw,
                                    ordered=ordered)

    return (('/'.join(path_tuple), json_dumps(l)) for path_tuple, l in rows)


def checksum(*, source='tables', file_order=False, name=None,
             file_means_path=True):

    kwargs = _json_kwargs(source=source,
                          file_order=file_order,
                          file_means_path=file_means_path)

    return _checksum(name=name, **kwargs)


def _checksum(bind_or_root=ENGINE, *, from_raw=False, ordered='id',
              name=None, dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    log.info('calculate languoids json checksum')

    rows = _json_rows(bind_or_root, from_raw=from_raw,
                      ordered=ordered, sort_keys=True)

    header = ['path', 'json']
    log.info('header: %r', header)

    hash_ = _queries.hash_rows(rows, header=header,
                               name=name, raw=True,
                               dialect=dialect, encoding=encoding)
    result = f"{'_'.join(header)}:{ordered}:{hash_.name}:{hash_.hexdigest()}"
    log.debug('%s: %r', hash_.name, result)
    return result
