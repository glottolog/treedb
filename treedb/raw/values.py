# values.py

import logging
import warnings

import csv23

import sqlalchemy as sa

from .. import ROOT, ENGINE

from .. import (tools as _tools,
                files as _files,
                queries as _queries)

from . import records as _records

from .models import File, Option, Value

__all__ = ['print_stats',
           'checksum',
           'write_raw_csv',
           'write_files']


log = logging.getLogger(__name__)


def print_stats(*, bind=ENGINE):
    log.info('fetch statistics')

    select_nvalues = sa.select([
            Option.section, Option.option, sa.func.count().label('n'),
        ])\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by('section', sa.desc('n'))

    template = '{section:<22} {option:<22} {n:,}'
    _queries.print_rows(select_nvalues, format_=template, bind=bind)


def checksum(*, weak=False, name=None,
             dialect=csv23.DIALECT, encoding=csv23.ENCODING, bind=ENGINE):
    kind = {True: 'weak', False: 'strong', 'unordered': 'unordered'}[weak]
    log.info('calculate %r raw checksum', kind)

    if weak:
        select_rows = sa.select([
                File.path, Option.section, Option.option, Value.value,
            ]).select_from(sa.join(File, Value).join(Option))\

        order = ['path', 'section', 'option']
        if weak == 'unordered':
            order.append(Value.value)
        else:
            order.append(Value.line)
        select_rows.append_order_by(*order)

    else:
        select_rows = sa.select([File.path, File.sha256]).order_by('path')

    hash_ = _queries.hash_csv(select_rows, raw=True, name=name,
                               dialect=dialect, encoding=encoding, bind=bind)

    logging.debug('%s: %r', hash_.name, hash_.hexdigest())
    return f'{kind}:{hash_.name}:{hash_.hexdigest()}'


def write_raw_csv(filename=None, *,
                  dialect=csv23.DIALECT, encoding=csv23.ENCODING, bind=ENGINE):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = bind.file_with_suffix('.raw.csv').name
    else:
        filename = _tools.path_from_filename(filename)

    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'deltete present file: {path!r}')
        path.unlink()

    select_values = sa.select([
            File.path, Option.section, Option.option, Value.line, Value.value,
        ]).select_from(sa.join(File, Value).join(Option))\
        .order_by('path', 'section', 'option', 'line')

    return _queries.write_csv(select_values, filename,
                              dialect=dialect, encoding=encoding, bind=bind)


def write_files(root=ROOT, *, replace=False, bind=ENGINE):
    """Write (path, section, option, line, value) rows back into config files."""
    log.info('write from raw records to tree')
    records = _records.iterrecords(bind=bind)
    return _files.write_files(records, root=root, replace=replace)
