"""Load, parse, serialize, and write ``md.ini`` files."""

import typing

from .. import _globals
from .. import _tools

from ._root import TREE_IN_ROOT, set_root, get_repo_root

from .files import iterfiles

__all__ = ['set_root', 'get_repo_root',
           'iterfiles',
           'iterrecords']


def iterrecords(root=_globals.ROOT,
                *, progress_after: int = _tools.PROGRESS_AFTER
                ) -> typing.Iterable[_globals.RecordItem]:
    for path_tuple, _, cfg in iterfiles(root, progress_after=progress_after):
        yield path_tuple, cfg
