# write information to stdout, csv, json, etc.

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

from ._globals import (DEFAULT_ENGINE,
                       PATH_LABEL, LANGUOID_LABEL,
                       ENGINE, ROOT,
                       LANGUOID_ORDER)

from . import _tools
from . import backend as _backend
from .backend import export as _export
from . import languoids as _languoids
from .models import SPECIAL_FAMILIES, BOOKKEEPING
from . import queries as _queries
from . import records as _records

__all__ = ['print_languoid_stats',
           'checksum',
           'write_json_lines',
           'write_json_csv',
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
    by_file = 'path' if file_means_path else 'file'
    ordered = by_file if file_order else 'id'
    try:
        return {'tables': {'root_or_bind': ENGINE,
                           'from_raw': False,
                           'ordered': ordered},
                'raw': {'root_or_bind': ENGINE,
                        'from_raw': True,
                        'ordered': ordered},
                'files': {'root_or_bind': ROOT,
                          'from_raw': None,
                          'ordered': 'path'}}[source]
    except KeyError:
        raise ValueError(f'unknown checksum source: {source!r}'
                         f' (possible values: {list(kwargs)})')


def checksum(*, name=None,
             source='tables',
             file_order: bool = False,
             file_means_path: bool = True):
    """Return checksum over source."""
    kwargs = validate_source_kwargs(source=source,
                                    file_order=file_order,
                                    file_means_path=file_means_path)

    return _checksum(name=name, **kwargs)


def _checksum(root_or_bind=ENGINE, *, name=None, ordered='id',
              from_raw: bool = False,
              dialect=csv23.DIALECT, encoding: str = csv23.ENCODING):
    log.info('calculate languoids json checksum')

    rows = _languoids.iterlanguoids(root_or_bind,
                                    ordered=ordered,
                                    from_raw=from_raw,
                                    _legacy=True)

    rows = pipe_json('dump', rows,
                     sort_keys=True)

    header = ['path', 'json']
    log.info('csv header: %r', header)

    hash_ = _export.hash_rows(rows, header=header,
                              name=name, raw=True,
                              dialect=dialect, encoding=encoding)
    result = f"{'_'.join(header)}:{ordered}:{hash_.name}:{hash_.hexdigest()}"
    log.debug('%s: %r', hash_.name, result)
    return result


def write_json_lines(file=None, *, suffix='.jsonl',
                     delete_present: bool = True,
                     autocompress: bool = True,
                     source='tables',
                     file_order: bool = True,
                     file_means_path: bool = True,
                     sort_keys: bool = True,
                     path_label: str = PATH_LABEL,
                     languoid_label: str = LANGUOID_LABEL):
    r"""Write languoids as newline delimited JSON.

    $ python -c "import sys, treedb; treedb.load('treedb.sqlite3'); treedb.write_json_lines(sys.stdout)" \
    | jq -s "group_by(.languoid.level)[]| {level: .[0].languoid.level, n: length}"

    $ jq "del(recurse | select(. == null or arrays and empty))" treedb.languoids.jsonl > treedb.languoids-jq.jsonl
    """
    lang_kwargs = validate_source_kwargs(source=source,
                                         file_order=file_order,
                                         file_means_path=file_means_path)

    if file is None:
        if source == 'files':
            file = _tools.path_from_filename(DEFAULT_ENGINE)
        else:
            root_or_bind = lang_kwargs['root_or_bind']
            if hasattr(root_or_bind, 'file_with_name'):
                file = root_or_bind.file
            else:
                file = _tools.path_from_filename(root_or_bind)
        file = file.with_name(f'{file.stem}-{source}.languoids{suffix}')

    log.info('write json lines: %r', file)

    json_kwargs = {'delete_present': delete_present,
                   'autocompress': autocompress}

    if source == 'tables':
        query_kwargs = {'as_rows': False,
                        'load_json': False,
                        'ordered': lang_kwargs['ordered']}

        query = _queries.get_json_query(sort_keys=sort_keys,
                                        path_label=path_label,
                                        languoid_label=languoid_label,
                                        **query_kwargs)

        with _backend.connect(bind=lang_kwargs['root_or_bind']) as conn:
            lines = conn.execute(query).scalars()
            result = _tools.pipe_json_lines(file, lines, raw=True,
                                            **json_kwargs)
        return result
        
    items = _languoids.iterlanguoids(**lang_kwargs)
    items = ({path_label: path, languoid_label: languoid}
             for path, languoid in items)
    return _tools.pipe_json_lines(file, items,
                                  sort_keys=sort_keys,
                                  **json_kwargs)


# DEPRECATED
def write_json_csv(*, filename=None, sort_keys: bool = True,
                   source='tables',
                   file_order: bool = False,
                   file_means_path: bool = True,
                   dialect=csv23.DIALECT, encoding: str = csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    kwargs = validate_source_kwargs(source=source,
                                    file_order=file_order,
                                    file_means_path=file_means_path)

    return _write_json_csv(filename=filename, sort_keys=sort_keys,
                           dialect=csv23.DIALECT, encoding=csv23.ENCODING,
                           **kwargs)


def _write_json_csv(root_or_bind=ENGINE, *, filename=None, ordered: bool = True,
                    sort_keys: bool = True, from_raw: bool = False,
                    dialect=csv23.DIALECT, encoding: str = csv23.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    if filename is None:
        suffix = '.languoids-json.csv.gz'
        try:
            path = root_or_bind.file_with_suffix(suffix)
        except AttributeError:
            path = _tools.path_from_filename(root_or_bind).with_suffix(suffix)
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

    kwargs = {'ordered': ordered,
              'from_raw':from_raw}

    rows = _languoids.iterlanguoids(root_or_bind, _legacy=True,
                                    **kwargs)
    rows = pipe_json('dump', rows, sort_keys=sort_keys)

    return csv23.write_csv(filename, rows, header=header,
                           dialect=dialect, encoding=encoding,
                           autocompress=True)


def pipe_json(mode, languoids, *,
              sort_keys: bool = True):
    codec = {'load': json.loads, 'dump': json.dumps}[mode]

    if mode == 'dump':
        codec = functools.partial(codec,
                                  # json-serialize datetime.datetime
                                  default=operator.methodcaller('isoformat'),
                                  sort_keys=sort_keys)

    if mode == 'dump':
        def itercodec(langs):
            for path_tuple, l in langs:
                yield '/'.join(path_tuple), codec(l)
    else:
        def itercodec(langs):
            for path, doc in langs:
                yield path.split('/'), codec(doc)

    return itercodec(languoids)


def fetch_languoids(bind=ENGINE, *, ordered=LANGUOID_ORDER,
                    progress_after: int = _tools.PROGRESS_AFTER,
                    _legacy=None):
    log.info('fetch languoids from json query')
    log.info('ordered: %r', ordered)

    kwargs = {'as_rows': True,
              'load_json': True,
              'ordered': ordered}

    query = _queries.get_json_query(_legacy=_legacy, **kwargs)

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


def write_files(root=ROOT, *, replace: bool = False,
                from_raw: bool = False,
                progress_after: int = _tools.PROGRESS_AFTER,
                bind=ENGINE) -> int:
    log.info('write from tables to tree')

    from . import files

    kwargs = {'ordered': 'path',
              'from_raw': from_raw}

    languoids = _languoids.iterlanguoids(bind, **kwargs)

    records = _records.dump(languoids)

    return files.write_files(records, root=root, _join_lines=True,
                             replace=replace,
                             progress_after=progress_after)
