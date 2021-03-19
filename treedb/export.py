# export.py - load languoids from tables and write json csv

import functools
import json
import logging
import operator
import warnings

import csv23

from . import _compat

from . import ENGINE, ROOT

from . import _tools
from . import backend as _backend
from .backend import export as _export
from . import languoids as _languoids
from . import queries as _queries

__all__ = ['checksum',  'write_json_csv',
           'iterlanguoids']


log = logging.getLogger(__name__)


def validate_source_kwargs(*, source, file_order, file_means_path):
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


def checksum(*, name=None,
             source='tables', file_order=False, file_means_path=True):
    """Return checksum over source."""
    kwargs = validate_source_kwargs(source=source,
                                    file_order=file_order,
                                    file_means_path=file_means_path)

    return _checksum(name=name, **kwargs)


def write_json_csv(*, filename=None, sort_keys=True,
                   source='tables', file_order=False, file_means_path=True,
                   dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    kwargs = validate_source_kwargs(source=source,
                                    file_order=file_order,
                                    file_means_path=file_means_path)

    return _write_json_csv(filename=filename, sort_keys=sort_keys,
                           dialect=csv23.DIALECT, encoding=csv23.ENCODING,
                           **kwargs)


def _checksum(bind_or_root=ENGINE, *, name=None,  ordered='id',
              from_raw=False,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    log.info('calculate languoids json checksum')

    rows = _json_rows(bind_or_root, from_raw=from_raw,
                      ordered=ordered, sort_keys=True)

    header = ['path', 'json']
    log.info('csv header: %r', header)

    hash_ = _export.hash_rows(rows, header=header,
                              name=name, raw=True,
                              dialect=dialect, encoding=encoding)
    result = f"{'_'.join(header)}:{ordered}:{hash_.name}:{hash_.hexdigest()}"
    log.debug('%s: %r', hash_.name, result)
    return result


def _write_json_csv(bind_or_root=ENGINE, *, filename=None, ordered=True,
                    sort_keys=True, from_raw=False,
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
    log.info('csv header: %r', header)

    rows = _json_rows(bind_or_root,
                      ordered=ordered,
                      sort_keys=sort_keys,
                      from_raw=from_raw)

    return csv23.write_csv(filename, rows, header=header,
                           dialect=dialect, encoding=encoding,
                           autocompress=True)


def _json_rows(bind_or_root=ENGINE, *,
               ordered=True, sort_keys=True,
               from_raw=False):
    json_dumps = functools.partial(json.dumps,
                                   # json-serialize datetime.datetime
                                   default=operator.methodcaller('isoformat'),
                                   sort_keys=sort_keys)

    rows = _languoids.iterlanguoids(bind_or_root,
                                    from_raw=from_raw,
                                    ordered=ordered)

    return (('/'.join(path_tuple), json_dumps(l)) for path_tuple, l in rows)



def iterlanguoids(bind=ENGINE, *, ordered='id',
                  progress_after=_tools.PROGRESS_AFTER):
    log.info('select languoids from json query')
    log.info('ordered: %r', ordered)

    query = _queries.get_json_query(ordered=ordered,
                                    load_json=True)

    json_datetime = _compat.datetime_fromisoformat

    rows = _backend.iterrows(query, bind=bind)

    n = 0
    for n, (path, item) in enumerate(rows, 1):
        endangerment = item['endangerment']
        if endangerment is not None:
            endangerment['date'] = json_datetime(endangerment['date'])
        if not item.get('timespan'):
            item.pop('timespan', None)

        yield tuple(path.split('/')), item

        if not (n % progress_after):
            log.info('%s languoids fetched', f'{n:_d}')

    log.info('%s languoids total', f'{n:_d}')
