# export.py - load languoids from tables and write json csv

import functools
import io
import itertools
import json
import logging
import operator
import sys
import warnings

import csv23

from . import _compat

from . import ENGINE, ROOT

from . import _tools
from . import backend as _backend
from .backend import export as _export
from . import languoids as _languoids
from .models import SPECIAL_FAMILIES, BOOKKEEPING
from . import queries as _queries
from . import records as _records

__all__ = ['print_languoid_stats',
           'checksum',
           'write_json_csv',
           'write_json_query_csv',
           'write_json_lines',
           'fetch_languoids',
           'write_files']


log = logging.getLogger(__name__)


def print_languoid_stats(*, file=None,
                         bind=ENGINE):
    rows = _backend.iterrows(_queries.get_stats_query(),
                             mappings=True, bind=bind)
    rows, counts = itertools.tee(rows)

    _export.print_rows(rows,
                       format_='{n:6,d} {kind}',
                       file=file,
                       bind=None)

    sums = [('languoids', ('families', 'languages', 'subfamilies', 'dialects')),
            ('roots', ('families', 'isolates')),
            ('All', ('Spoken L1 Languages',) + SPECIAL_FAMILIES),
            ('languages', ('All', BOOKKEEPING))]

    counts = {c['kind']: c['n'] for c in counts}
    for total, parts in sums:
        values = [counts[p] for p in parts]
        parts_sum = sum(values)
        term = ' + '.join(f'{v:,d} {p}' for p, v in zip(parts, values))
        log.debug('verify %s == %d %s', term, counts[total], total)
        if counts[total] != parts_sum:  # pragma: no cover
            warnings.warn(f'{term} = {parts_sum:,d}')


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


def _checksum(bind_or_root=ENGINE, *, name=None, ordered='id',
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


def write_json_query_csv(filename=None, *,
                         ordered='id', raw=False,
                         bind=ENGINE):
    if filename is None:
        suffix = '_raw' if raw else ''
        suffix = f'.languoids-json_query{suffix}.csv.gz'
        filename = bind.file_with_suffix(suffix).name

    query = _queries.get_json_query(ordered=ordered,
                                    as_rows=True,
                                    load_json=raw)

    return _export.write_csv(query, filename=filename, bind=bind)


def write_json_lines(filename=None, *,
                     bind=ENGINE, _encoding='utf-8'):
    r"""Write languoids as newline delimited JSON.

    $ python -c "import sys, treedb; treedb.load('treedb.sqlite3'); treedb.write_json_lines(sys.stdout)" \
    | jq -s "group_by(.languoid.level)[]| {level: .[0].languoid.level, n: length}"

    $ jq "del(recurse | select(. == null or arrays and empty))" treedb.languoids.jsonl > treedb.languoids-jq.jsonl
    """
    open_kwargs = {'encoding': _encoding}

    path = fobj = None
    if filename is None:
        path = bind.file_with_suffix('.languoids.jsonl')
    elif filename is sys.stdout:
        fobj = io.TextIOWrapper(sys.stdout.buffer, **open_kwargs)
    elif hasattr(filename, 'write'):
        fobj = filename
    else:
        path = _tools.path_from_filename(filename)

    if path is None:
        log.info('write json lines into: %r', fobj)
        open_func = lambda: _compat.nullcontext(fobj)
        result = fobj
    else:
        log.info('write json lines: %r', path)
        open_func = functools.partial(path.open, 'wt', **open_kwargs)
        result = path
        if path.exists():
            warnings.warn(f'delete present file: {path!r}')
            path.unlink()

    assert result is not None

    query = _queries.get_json_query(ordered='id',
                                    as_rows=False,
                                    load_json=False,
                                    languoid_label='languoid')

    rows = _backend.iterrows(query, bind=bind)

    with open_func() as f:
        write_line = functools.partial(print, file=f)
        for path_languoid, in rows:
            write_line(path_languoid)

    return result


def fetch_languoids(bind=ENGINE, *, ordered='id',
                    progress_after=_tools.PROGRESS_AFTER):
    log.info('fetch languoids from json query')
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


def write_files(root=ROOT, *, replace=False,
                from_raw=False,
                progress_after=_tools.PROGRESS_AFTER, bind=ENGINE):
    log.info('write from tables to tree')

    from . import files

    languoids = _languoids.iterlanguoids(bind,
                                         from_raw=from_raw,
                                         ordered='path')
    records = _records.records_from_languoids(languoids)

    return files.write_files(records, root=root, replace=replace,
                             progress_after=progress_after)
