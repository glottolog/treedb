# write information to stdout, csv, files

import logging
import typing
import warnings

import csv23

import sqlalchemy as sa

from .. import _globals
from .. import _tools
from ..backend import export as _backend_export
from .. import files as _files

from . import records as _records

from .models import File, Option, Value

__all__ = ['print_stats',
           'checksum',
           'write_raw_csv',
           'write_files']


log = logging.getLogger(__name__)


def print_stats(*, file=None):
    log.info('fetch statistics')

    # order by descending frequency for any_options and undefined options
    select_nvalues = (sa.select(Option.section, Option.option,
                                sa.func.count().label('n'))
                      .join_from(Option, Value)
                      .group_by(Option.section, Option.option)
                      .order_by(sa.desc('defined'),
                                'ord_section', 'ord_option',
                                'section', sa.desc('n'), 'option'))

    template = '{section:<22} {option:<22} {n:,}'

    _backend_export.print_rows(select_nvalues, format_=template,
                               file=file)


def checksum(*, weak: bool = False,
             hash_name: str = _globals.DEFAULT_HASH,
             dialect: str = csv23.DIALECT,
             encoding: str = csv23.ENCODING):
    kind = {True: 'weak', False: 'strong', 'unordered': 'unordered'}[weak]
    log.info('calculate %r raw checksum', kind)

    if weak:
        select_rows = (sa.select(File.path,
                                 Option.section, Option.option,
                                 Value.value)
                       .join_from(File, Value).join(Option))

        order = ['path', 'section', 'option']
        if weak == 'unordered':
            order.append(Value.value)
        else:
            order.append(Value.line)
        select_rows = select_rows.order_by(*order)

    else:
        select_rows = (sa.select(File.path, File.sha256)
                       .order_by('path'))

    hashobj = _backend_export.hash_csv(select_rows,
                                       hash_name=hash_name,
                                       dialect=dialect, encoding=encoding,
                                       raw=True)

    logging.info('%s: %r', hashobj.name, hashobj.hexdigest())
    return f'{kind}:{hashobj.name}:{hashobj.hexdigest()}'


def write_raw_csv(filename=None, *,
                  dialect: str = csv23.DIALECT, encoding: str = csv23.ENCODING):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = _globals.ENGINE.file_with_suffix('.raw.csv.gz').name
    else:
        filename = _tools.path_from_filename(filename)

    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'deltete present file: {path!r}')
        path.unlink()

    select_values = (sa.select(File.path,
                               Option.section, Option.option,
                               Value.line, Value.value)
                     .join_from(File, Value).join(Option)
                     .order_by('path', 'section', 'option', 'line'))

    return _backend_export.write_csv(select_values, filename,
                                     dialect=dialect, encoding=encoding)


def write_files(root=_globals.ROOT, *, replace: bool = False,
                dry_run: bool = False,
                require_nwritten: typing.Optional[int] = None,
                limit: typing.Optional[int] = None,
                offset: typing.Optional[int] = 0,
                progress_after: int = _tools.PROGRESS_AFTER,
                bind=_globals.ENGINE):
    """Write (path, section, option, line, value) rows back into config files."""
    log.info('write from raw records to tree')

    records = _records.fetch_records(bind=bind)
    records = _tools.islice_limit(records,
                                  limit=limit,
                                  offset=offset)

    return _files.write_files(records, root=root, replace=replace,
                              dry_run=dry_run,
                              require_nwritten=require_nwritten,
                              progress_after=progress_after)
