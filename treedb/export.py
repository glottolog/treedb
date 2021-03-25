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

SOURCES = ('files', 'raw', 'tables')


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


def iterlanguoids(source: str = 'files',
                  *, limit: typing.Optional[int] = None,
                  offset: typing.Optional[int] = 0,
                  order_by: str = LANGUOID_ORDER,
                  progress_after: int = _tools.PROGRESS_AFTER,
                  root=ROOT, bind=ENGINE) -> typing.Iterable[LanguoidItem]:
    """Yield (path, languoid) pairs from diffferent sources."""
    log.info('generate languoids')
    if source not in SOURCES:
        raise ValueError(f'unknown source: {source!r}')

    if source == 'files':
        log.info('extract languoids from files')

        from . import files

        if order_by not in (True, None, False, 'file', 'path'):
            raise ValueError(f'order_by={order_by!r} not implemented')
        del order_by

        records = files.iterrecords(root=root,
                                    progress_after=progress_after)
    elif source == 'raw':
        log.info('extract languoids from raw records')

        from . import raw

        records = raw.fetch_records(order_by=order_by,
                                    progress_after=progress_after,
                                    bind=bind)
        del order_by
    else:
        return fetch_languoids(limit=limit,
                               offset=offset,
                               order_by=order_by,
                               progress_after=progress_after,
                               bind=bind)


    items = _records.parse(records, from_raw=(source == 'raw'))
    if limit is not None and offset:
        return itertools.islice(items, limit, limit + offset)
    elif limit is not None:
        return itertools.islice(items, limit)
    return items


def checksum(source: str = 'tables',
             *, order_by: str = LANGUOID_ORDER,
             hash_name: str = DEFAULT_HASH,
             bind=ENGINE):
    """Return checksum over source."""
    log.info('calculate languoids json checksum')
    if source not in SOURCES:
        raise ValueError(f'unknown source: {source!r}')

    log.info('hash json lines with %r', hash_name)
    hashobj = hashlib.new(hash_name)
    assert hasattr(hashobj, 'hexdigest')
    hashobj, total_lines = write_json_lines(hashobj,
                                            source=source,
                                            order_by=order_by,
                                            sort_keys=True,
                                            bind=bind)
    log.info('%d lines written', total_lines)
    name = 'path_languoid'

    result = (f'{name}'
              f':{order_by}'
              f':{hashobj.name}'
              f':{hashobj.hexdigest()}')
    log.info('%s: %r', hashobj.name, result)
    return result


def write_json_lines(file=None, *, suffix: str = '.jsonl',
                     delete_present: bool = True,
                     autocompress: bool = True,
                     source: str = 'tables',
                     order_by: str = LANGUOID_ORDER,
                     sort_keys: bool = True,
                     path_label: str = PATH_LABEL,
                     languoid_label: str = LANGUOID_LABEL,
                     bind=ENGINE):
    r"""Write languoids as newline delimited JSON.

    $ python -c "import sys, treedb; treedb.load('treedb.sqlite3'); treedb.write_json_lines(sys.stdout)" \
    | jq -s "group_by(.languoid.level)[]| {level: .[0].languoid.level, n: length}"

    $ jq "del(recurse | select(. == null or arrays and empty))" treedb.languoids.jsonl > treedb.languoids-jq.jsonl
    """
    if source not in SOURCES:
        raise ValueError(f'unknown source: {source!r}')

    if file is None:
        file = (_tools.path_from_filename(DEFAULT_ENGINE) if source == 'files'
                else bind.file)
        file = file.with_name(f'{file.stem}-{source}.languoids{suffix}')

    log.info('write json lines: %r', file)

    if source == 'tables':
        query = _queries.get_json_query(as_rows=False,
                                        load_json=False,
                                        order_by=order_by,
                                        sort_keys=sort_keys,
                                        path_label=path_label,
                                        languoid_label=languoid_label)

        del sort_keys

        with _backend.connect(bind=bind) as conn:
            lines = conn.execute(query).scalars()
            result = _tools.pipe_json_lines(file, lines,
                                            raw=True,
                                            delete_present=delete_present,
                                            autocompress=autocompress)
        return result

    items = iterlanguoids(source, order_by=order_by, bind=bind)
    items = ({path_label: path, languoid_label: languoid}
             for path, languoid in items)
    return _tools.pipe_json_lines(file, items,
                                  sort_keys=sort_keys,
                                  delete_present=delete_present,
                                  autocompress=autocompress)


def fetch_languoids(*, limit: typing.Optional[int] = None,
                    offset: typing.Optional[int] = 0,
                    order_by: str = LANGUOID_ORDER,
                    progress_after: int = _tools.PROGRESS_AFTER,
                    bind=ENGINE):
    log.info('fetch languoids from json query')
    log.info('order_by: %r', order_by)

    query = _queries.get_json_query(order_by=order_by,
                                    as_rows=True,
                                    load_json=True)

    if offset:
        query = query.offset(offset)
        del offset

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
                source: str = 'tables',
                progress_after: int = _tools.PROGRESS_AFTER,
                bind=ENGINE) -> int:
    log.info('write from tables to tree')

    from . import files

    languoids = iterlanguoids(source, order_by='path', bind=bind)

    records = _records.dump(languoids)

    return files.write_files(records, root=root, _join_lines=True,
                             replace=replace,
                             progress_after=progress_after)
