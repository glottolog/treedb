# write information to stdout, csv, json, etc.

import itertools
import hashlib
import logging
import typing
import warnings

from . import _compat

from ._globals import (DEFAULT_ENGINE, DEFAULT_HASH,
                       PATH_LABEL, LANGUOID_LABEL, LANGUOID_ORDER,
                       ENGINE, ROOT,
                       LanguoidItem)

from . import _tools
from . import backend as _backend
from .backend import export as _export
from .models import SPECIAL_FAMILIES, BOOKKEEPING
from . import queries as _queries
from . import records as _records

__all__ = ['print_languoid_stats',
           'iterlanguoids',
           'checksum',
           'write_json_lines',
           'fetch_languoids',
           'write_files']


log = logging.getLogger(__name__)


def print_languoid_stats(*, file=None,
                         bind=ENGINE):
    select_stats = _queries.get_stats_query()
    rows = _backend.iterrows(select_stats, mappings=True, bind=bind)
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


def iterlanguoids(root_or_bind=ROOT, *,
                  limit: typing.Optional[int] = None,
                  from_raw: bool = False,
                  ordered: bool = True,
                  progress_after: int = _tools.PROGRESS_AFTER) -> typing.Iterable[LanguoidItem]:
    """Yield (path, languoid) pairs from diffferent sources."""
    kwargs = {'progress_after': progress_after}
    log.info('generate languoids')
    if not hasattr(root_or_bind, 'execute'):
        log.info('extract languoids from files')
        if from_raw:
            raise TypeError(f'from_raw=True requires bind'
                            f' (passed: {root_or_bind!r})')

        if ordered not in (True, None, False, 'file', 'path'):
            raise ValueError(f'ordered={ordered!r} not implemented')

        from . import files

        del ordered
        records = files.iterrecords(root=root_or_bind, **kwargs)
    elif not from_raw:
        kwargs['ordered'] = ordered
        return fetch_languoids(bind=root_or_bind, limit=limit, **kwargs)
    else:
        log.info('extract languoids from raw records')

        from . import raw

        records = raw.fetch_records(bind=root_or_bind, **kwargs)

    items = _records.parse(records, from_raw=from_raw)
    return itertools.islice(items, limit) if limit is not None else items


def get_source_kwargs(*, source: str,
                      file_order: bool,
                      file_means_path: bool):
    if source == 'files':
        return {'root_or_bind': ROOT,
                'from_raw': None,
                'ordered': 'path'}
    elif source in ('raw', 'tables'):
        return {'root_or_bind': ENGINE,
                'from_raw': (source == 'raw'),
                'ordered': ('path' if file_order and file_means_path else
                            'file' if file_order else
                            'id')}
    else:
        raise ValueError(f'unknown source: {source!r}')


def checksum(source: str = 'tables', *,
             hash_name: str = DEFAULT_HASH):
    """Return checksum over source."""
    log.info('calculate languoids json checksum')
    kwargs = get_source_kwargs(source=source,
                               file_order=True,
                               file_means_path=True)

    log.info('hash json lines with %r', hash_name)
    hashobj = hashlib.new(hash_name)
    assert hasattr(hashobj, 'hexdigest')
    kwargs['ordered'] = 'path'
    hashobj, total_lines = write_json_lines(hashobj,
                                            source=source,
                                            file_order=True,
                                            file_means_path=True,
                                            sort_keys=True)
    log.info('%d lines written', total_lines)
    name = 'path_languoid'

    result = (f"{name}"
              f":{kwargs['ordered']}"
              f':{hashobj.name}'
              f':{hashobj.hexdigest()}')
    log.info('%s: %r', hashobj.name, result)
    return result


def write_json_lines(file=None, *, suffix: str = '.jsonl',
                     delete_present: bool = True,
                     autocompress: bool = True,
                     source: str = 'tables',
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
    lang_kwargs = get_source_kwargs(source=source,
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

    items = iterlanguoids(**lang_kwargs)
    items = ({path_label: path, languoid_label: languoid}
             for path, languoid in items)
    return _tools.pipe_json_lines(file, items,
                                  sort_keys=sort_keys,
                                  **json_kwargs)


def fetch_languoids(*, limit: typing.Optional[int] = None,
                    ordered: str = LANGUOID_ORDER,
                    progress_after: int = _tools.PROGRESS_AFTER,
                    bind=ENGINE):
    log.info('fetch languoids from json query')
    log.info('ordered: %r', ordered)

    kwargs = {'as_rows': True,
              'load_json': True,
              'ordered': ordered}

    query = _queries.get_json_query(**kwargs)

    if limit is not None:
        query = query.limit(limit)
        del limit

    json_datetime = _compat.datetime_fromisoformat

    rows = _backend.iterrows(query, bind=bind)

    n = 0
    make_item = LanguoidItem.from_filepath_languoid
    for n, (path, item) in enumerate(rows, 1):
        endangerment = item['endangerment']
        if endangerment is not None:
            endangerment['date'] = json_datetime(endangerment['date'])
        if not item.get('timespan'):
            item.pop('timespan', None)
        yield make_item(path, item)

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

    languoids = iterlanguoids(bind, **kwargs)

    records = _records.dump(languoids)

    return files.write_files(records, root=root, _join_lines=True,
                             replace=replace,
                             progress_after=progress_after)
