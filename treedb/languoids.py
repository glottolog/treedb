# fetch languoids from selecteable sources

import logging
import typing

from ._globals import ROOT

from . import _globals
from . import _tools
from . import records as _records

__all__ = ['iterlanguoids']


log = logging.getLogger(__name__)


def iterlanguoids(root_or_bind=ROOT, *,
                  from_raw: bool = False,
                  ordered: bool = True,
                  progress_after: int = _tools.PROGRESS_AFTER
                  ) -> typing.Iterable[_globals.LanguoidItem]:
    """Yield dicts from languoids/tree/**/md.ini files."""
    log.info('generate languoids')

    if not hasattr(root_or_bind, 'execute'):
        log.info('extract languoids from files')
        root = root_or_bind

        if from_raw:
            raise TypeError(f'from_raw=True requires bind (passed: {root!r})')

        if ordered not in (True, False, 'file', 'path'):
            raise ValueError(f'ordered={ordered!r} not implemented')

        from . import files

        fileinfos = files.iterfiles(root, progress_after=progress_after)
        records = ((pt, cfg) for pt, cfg, _ in fileinfos)
    else:
        bind = root_or_bind

        if not from_raw:
            from . import export

            return export.fetch_languoids(bind,
                                          ordered=ordered,
                                          progress_after=progress_after)

        log.info('extract languoids from raw records')

        from . import raw

        if ordered is True:  # insert languoids in id order if available
            ordered = 'id'

        records = raw.fetch_records(bind=bind,
                                    ordered=ordered,
                                    progress_after=progress_after)

    return _records.parse(records, from_raw=from_raw)
