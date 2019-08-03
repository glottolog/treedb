# values.py

from .._compat import ENCODING, iteritems

import sqlalchemy as sa

from .. import ENGINE, ROOT

from .. import files as _files
from .. import queries as _queries

from . import records as _records

from .models import File, Option, Value, Fields

__all__ = ['print_stats', 'to_raw_csv', 'to_files']


def print_stats(bind=ENGINE):
    select_nvalues = sa.select([
            Option.section, Option.option, sa.func.count().label('n'),
        ], bind=bind)\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by(Option.section, sa.desc('n'))

    template = '{section:<22} {option:<22} {n:,}'
    _queries.print_rows(select_nvalues, format_=template, bind=bind)


def to_raw_csv(filename=None, encoding=ENCODING, bind=ENGINE):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = bind.file_with_suffix('.raw.csv').parts[-1]

    select_values = sa.select([
            File.path, Option.section, Option.option, Value.line, Value.value,
        ]).select_from(sa.join(File, Value).join(Option))\
        .order_by(File.path, Option.section, Option.option, Value.line)

    return _queries.write_csv(select_values, filename, encoding, bind=bind)


def to_files(root=ROOT, bind=ENGINE, verbose=True, is_lines=Fields.is_lines):
    """Write (path, section, option, line, value) rows back into config files."""
    records = _records.iterrecords(bind)

    def _iterpairs(records):
        for p, r in records:
            for section, s in iteritems(r):
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield p, r

    return _files.save(_iterpairs(records), root, verbose=verbose)
