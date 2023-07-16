"""Set the root for the ``languoids/tree/**/md.ini`` file tree."""

import logging

from .. import _globals
from .. import _tools

__all__ = ['set_root', 'get_repo_root']

TREE_IN_ROOT = _tools.path_from_filename('languoids', 'tree')


log = logging.getLogger(__name__)


def set_root(repo_root, /, *, resolve: bool = False,
             treepath=TREE_IN_ROOT):
    """Set and return default root for glottolog lanugoid directory tree."""
    log.info('set_root: %r', repo_root)
    if repo_root is None:
        raise ValueError(f'missing repo_root path: {repo_root!r}')

    repo_path = _tools.path_from_filename(repo_root)
    if resolve:
        repo_path = repo_path.resolve(strict=False)

    ROOT = _globals.ROOT  # noqa: N806
    ROOT.path = repo_path / _tools.path_from_filename(treepath)
    return ROOT


def get_repo_root(root=_globals.ROOT, /, *,
                  treepath=TREE_IN_ROOT):
    assert root.parts[-len(treepath.parts):] == treepath.parts
    repo_root = _tools.path_from_filename(root)
    for _ in treepath.parts:
        repo_root = repo_root.parent
    return repo_root
