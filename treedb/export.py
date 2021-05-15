"""Yield languoids, write information to stdout, .csv, .jsonl, etc."""

import itertools
import hashlib
import logging
import operator
import typing
import warnings

from . import _compat

from . import _globals
from . import _tools
from . import backend as _backend
from .backend import export as _backend_export
from .backend import pandas as _backend_pandas
from .models import SPECIAL_FAMILIES, BOOKKEEPING
from . import queries as _queries
from . import records as _records

__all__ = ['print_languoid_stats',
           'iterlanguoids',
           'checksum',
           'write_json_lines',
           'pd_read_languoids',
           'fetch_languoids',
           'write_files']

CHECKSUM_NAME = 'path_languoid'

FALLBACK_ENGINE_PATH = _tools.path_from_filename('treedb.sqlite3')


log = logging.getLogger(__name__)


def print_languoid_stats(*, file=None,
                         bind=_globals.ENGINE):
    select_stats = _queries.get_stats_query()
    rows = _backend.iterrows(select_stats, mappings=True, bind=bind)
    rows, counts = itertools.tee(rows)

    _backend_export.print_rows(rows, format_='{n:6,d} {kind}',
                               file=file, bind=None)

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
                  order_by: str = _globals.LANGUOID_ORDER,
                  progress_after: int = _tools.PROGRESS_AFTER,
                  root=_globals.ROOT, bind=_globals.ENGINE,
                  ) -> typing.Iterable[_globals.LanguoidItem]:
    """Yield (path, languoid) pairs from diffferent sources."""
    log.info('generate languoids from %r', source)
    if source in ('files', 'raw'):
        log.info('extract languoids from %r', source)
        if source == 'files':
            if order_by not in ('path', 'file', True, None, False):  # pragma: no cover
                raise ValueError(f'order_by={order_by!r} not implemented')
            else:
                del order_by

            from . import files

            records = files.iterrecords(root=root,
                                        progress_after=progress_after)
        elif source == 'raw':
            from . import raw

            records = raw.fetch_records(order_by=order_by,
                                        progress_after=progress_after,
                                        bind=bind)

        items = _records.pipe(records, dump=False,
                              convert_lines=(source == 'files'))
        return _tools.islice_limit(items,
                                   limit=limit,
                                   offset=offset)
    elif source == 'tables':
        return fetch_languoids(limit=limit,
                               offset=offset,
                               order_by=order_by,
                               progress_after=progress_after,
                               bind=bind)
    else:  # pragma: no cover
        raise ValueError(f'unknown source: {source!r}')


def checksum(source: str = 'tables',
             *, limit: typing.Optional[int] = None,
             offset: typing.Optional[int] = 0,
             order_by: str = _globals.LANGUOID_ORDER,
             hash_name: str = _globals.DEFAULT_HASH,
             bind=_globals.ENGINE):
    """Return checksum over source."""
    log.info('hash languoids json lines from %r ordered by %r with %r',
             source, order_by, hash_name)
    hashobj = hashlib.new(hash_name)
    assert hasattr(hashobj, 'hexdigest')
    hashobj, total_lines = write_json_lines(hashobj,
                                            source=source,
                                            limit=limit,
                                            offset=offset,
                                            order_by=order_by,
                                            sort_keys=True, bind=bind)
    log.info('%s json lines written', f'{total_lines:_d}')

    offset = f'offset={offset!r}' if offset else ''
    limit = f'limit={limit!r}' if limit is not None else ''
    sliced = ','.join(s for s in (offset, limit) if s)
    sliced = f'[{sliced}]' if sliced else ''
    result = (f'{CHECKSUM_NAME}:{order_by}{sliced}'
              f':{hashobj.name}:{hashobj.hexdigest()}')
    log.info('%s: %r', hashobj.name, result)
    return result


def write_json_lines(file=None, *, suffix: str = '.jsonl',
                     delete_present: bool = True,
                     autocompress: bool = True,
                     source: str = 'tables',
                     limit: typing.Optional[int] = None,
                     offset: typing.Optional[int] = 0,
                     order_by: str = _globals.LANGUOID_ORDER,
                     sort_keys: bool = True,
                     path_label: str = _globals.PATH_LABEL,
                     languoid_label: str = _globals.LANGUOID_LABEL,
                     bind=_globals.ENGINE):
    r"""Write languoids as newline delimited JSON.

    $ python -c "import sys, treedb; treedb.load('treedb.sqlite3'); treedb.write_json_lines(sys.stdout)" \
    | jq -s "group_by(.languoid.level)[]| {level: .[0].languoid.level, n: length}"

    $ jq "del(recurse | select(. == null or arrays and empty))" treedb.languoids.jsonl > treedb.languoids-jq.jsonl
    """
    if file is None:
        file = FALLBACK_ENGINE_PATH if source == 'files' else bind.file
        file = file.with_name(f'{file.stem}-{source}.languoids{suffix}')

    log.info('write json lines: %r', file)
    if source in ('files', 'raw'):
        items = iterlanguoids(source,
                              limit=limit, offset=offset,
                              order_by=order_by, bind=bind)
        items = ({path_label: path, languoid_label: languoid}
                 for path, languoid in items)
        return _tools.pipe_json_lines(file, items,
                                      sort_keys=sort_keys,
                                      delete_present=delete_present,
                                      autocompress=autocompress)
    elif source == 'tables':
        query = _queries.get_json_query(limit=limit,
                                        offset=offset,
                                        as_rows=False,
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
    else:  # pragma: no cover
        raise ValueError(f'unknown source: {source!r}')


def pd_read_languoids(*, source: str = 'tables',
                      limit: typing.Optional[int] = None,
                      offset: typing.Optional[int] = 0,
                      order_by: str = _globals.LANGUOID_ORDER,
                      sort_keys: bool = True,
                      path_label: str = _globals.PATH_LABEL,
                      languoid_label: str = _globals.LANGUOID_LABEL,
                      bind=_globals.ENGINE,
                      **kwargs):
    log.info('read json lines with pd.read_json(lines=True)')
    if source in ('files', 'raw'):
        items = iterlanguoids(source,
                              limit=limit,
                              offset=offset,
                              order_by=order_by,
                              bind=bind)
        items = ({path_label: path, languoid_label: languoid}
                 for path, languoid in items)
        json_lines = _tools.pipe_json(items, dump=True)
        df = _backend_pandas._pd_read_json_lines(json_lines,
                                                 orient='record',
                                                 **kwargs)
    elif source == 'tables':
        query = _queries.get_json_query(limit=limit,
                                        offset=offset,
                                        as_rows=False,
                                        load_json=False,
                                        order_by=order_by,
                                        sort_keys=sort_keys,
                                        path_label=path_label,
                                        languoid_label=languoid_label)

        df = _backend_pandas.pd_read_json_lines(query,
                                                orient='record',
                                                bind=bind,
                                                **kwargs)
    if df is not None:
        df.rename(columns={_globals.PATH_LABEL: 'path'}, inplace=True)
        index = df['languoid'].map(operator.itemgetter('id')).rename('id')
        df.set_index(index, inplace=True, verify_integrity=True)
    return df


def fetch_languoids(*, limit: typing.Optional[int] = None,
                    offset: typing.Optional[int] = 0,
                    order_by: str = _globals.LANGUOID_ORDER,
                    progress_after: int = _tools.PROGRESS_AFTER,
                    bind=_globals.ENGINE):
    log.info('fetch languoids from json query, order_by: %r', order_by)
    query = _queries.get_json_query(limit=limit,
                                    offset=offset,
                                    order_by=order_by,
                                    as_rows=True,
                                    load_json=True)
    del limit, offset

    json_datetime = _compat.datetime_fromisoformat

    rows = _backend.iterrows(query, bind=bind)

    n = 0
    make_item = _globals.LanguoidItem.from_filepath_languoid
    for n, (path, item) in enumerate(rows, start=1):
        endangerment = item['endangerment']
        if endangerment is not None:
            endangerment['date'] = json_datetime(endangerment['date'])
        if not item.get('timespan'):
            item.pop('timespan', None)
        yield make_item(path, item)

        if not (n % progress_after):
            log.info('%s languoids fetched', f'{n:_d}')

    log.info('%s languoids total', f'{n:_d}')


def write_files(root=_globals.ROOT, *, replace: bool = False,
                dry_run: bool = False,
                require_nwritten: typing.Optional[int] = None,
                source: str = 'tables',
                limit: typing.Optional[int] = None,
                offset: typing.Optional[int] = 0,
                progress_after: int = _tools.PROGRESS_AFTER,
                bind=_globals.ENGINE) -> int:
    log.info('write from %r to tree %r', source, root)
    if source == 'files':  # pragma: no cover
        raise NotImplementedError('simultaneaous write and read of files')

    if source == 'raw_lines':
        from . import raw

        return raw.write_files(root, replace=replace,
                               dry_run=dry_run,
                               require_nwritten=require_nwritten,
                               limit=limit,
                               offset=offset,
                               progress_after=progress_after,
                               bind=bind)

    from . import files

    languoids = iterlanguoids(source,
                              limit=limit,
                              offset=offset,
                              order_by='path',
                              progress_after=progress_after,
                              bind=bind)

    records = _records.pipe(languoids, dump=True,
                            convert_lines=(source == 'files'))

    return files.write_files(records, root=root, replace=replace,
                             dry_run=dry_run,
                             require_nwritten=require_nwritten,
                             progress_after=progress_after)
