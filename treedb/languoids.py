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
                  progress_after: int = _tools.PROGRESS_AFTER,
                  _legacy=None) -> typing.Iterable[_globals.LanguoidItem]:
    """Yield dicts from languoids/tree/**/md.ini files."""
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
        from . import export

        kwargs['ordered'] = ordered
        return export.fetch_languoids(bind=root_or_bind,
                                      _legacy=_legacy, **kwargs)
    else:
        log.info('extract languoids from raw records')

        from . import raw

        # insert languoids in id order if available
        kwargs['ordered'] = 'id' if ordered is True else ordered
        records = raw.fetch_records(bind=root_or_bind, **kwargs)

    return _records.parse(records, from_raw=from_raw,
                          _legacy=_legacy)
