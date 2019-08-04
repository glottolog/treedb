# values.py

from __future__ import unicode_literals

from .._compat import DIALECT, ENCODING, iteritems

import logging

import sqlalchemy as sa

from .. import ENGINE, ROOT

from .. import files as _files
from .. import queries as _queries

from . import records as _records

from .models import File, Option, Value, Fields

__all__ = ['print_stats', 'checksum', 'to_raw_csv', 'to_files']


log = logging.getLogger(__name__)


def print_stats(bind=ENGINE):
    select_nvalues = sa.select([
            Option.section, Option.option, sa.func.count().label('n'),
        ], bind=bind)\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by('section', sa.desc('n'))

    template = '{section:<22} {option:<22} {n:,}'
    _queries.print_rows(select_nvalues, format_=template, bind=bind)


def checksum(weak=False, name=None, dialect=DIALECT, encoding=ENCODING,
             bind=ENGINE):
    kind = 'weak' if weak else 'strong'
    log.info('calculate %r raw checksum', kind)

    if weak:
        select_rows = sa.select([
                File.path, Option.section, Option.option, Value.value,
            ], bind=bind)\
            .select_from(sa.join(File, Value).join(Option))\
            .order_by('path', 'section', 'option', Value.line)
    else:
        select_rows = sa.select([
                File.path, File.sha256
            ], bind=bind).order_by('path')

    hash_ = _queries.hash_csv(select_rows, raw=True, name=name,
                               dialect=dialect, encoding=encoding, bind=bind)

    logging.debug('%s: %r', hash_.name, hash_.hexdigest())
    return '%s:%s:%s' % (kind, hash_.name, hash_.hexdigest())


def to_raw_csv(filename=None, dialect=DIALECT, encoding=ENCODING, bind=ENGINE):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = bind.file_with_suffix('.raw.csv').name

    select_values = sa.select([
            File.path, Option.section, Option.option, Value.line, Value.value,
        ]).select_from(sa.join(File, Value).join(Option))\
        .order_by(File.path, Option.section, Option.option, Value.line)

    return _queries.write_csv(select_values, filename,
                              dialect=dialect, encoding=encoding, bind=bind)


def to_files(root=ROOT, bind=ENGINE, verbose=True, is_lines=Fields.is_lines):
    """Write (path, section, option, line, value) rows back into config files."""
    log.info('write raw records to tree')
    records = _records.iterrecords(bind)

    def _iterpairs(records):
        for p, r in records:
            for section, s in iteritems(r):
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield p, r

    return _files.save(_iterpairs(records), root, verbose=verbose)
