# export.py

import logging
import warnings

import csv23

import sqlalchemy as sa

from .. import ENGINE, ROOT

from .. import files as _files
from .. import queries as _queries
from .. import tools as _tools

from . import records as _records

from .models import File, Option, Value

__all__ = ['write_raw_csv',
           'write_files']


log = logging.getLogger(__name__)


def write_raw_csv(filename=None, *,
                  dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = ENGINE.file_with_suffix('.raw.csv.gz').name
    else:
        filename = _tools.path_from_filename(filename)

    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'deltete present file: {path!r}')
        path.unlink()

    select_values = sa.select(File.path,
                              Option.section, Option.option,
                              Value.line, Value.value)\
                    .join_from(File, Value).join(Option)\
                    .order_by('path', 'section', 'option', 'line')

    return _queries.write_csv(select_values, filename,
                              dialect=dialect, encoding=encoding)


def write_files(root=ROOT, *,
                bind=ENGINE,
                replace=False,
                progress_after=_tools.PROGRESS_AFTER):
    """Write (path, section, option, line, value) rows back into config files."""
    log.info('write from raw records to tree')

    records = _records.iterrecords(bind=bind)

    return _files.write_files(records, root=root, replace=replace,
                              progress_after=progress_after)
