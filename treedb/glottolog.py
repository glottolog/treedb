"""Clone/checkout the Glottolog master repo."""

from __future__ import annotations

import argparse
import logging

from . import _globals
from . import _tools
from . import languoids as _languoids

__all__ = ['glottolog_version',
           'checkout_or_clone',
           'git_rev_parse', 'git_describe',
           'git_status', 'git_status_is_clean']

REPO_URL = 'https://github.com/glottolog/glottolog.git'


log = logging.getLogger(__name__)


def glottolog_version(root=_globals.ROOT) -> argparse.Namespace:
    return GlottologVersion.from_root(root)


class GlottologVersion(argparse.Namespace):

    @classmethod
    def from_root(cls, root) -> GlottologVersion:
        return cls.from_commit_describe(git_rev_parse(root),
                                        git_describe(root))

    @classmethod
    def from_commit_describe(cls, commit: str, describe: str
                             ) -> GlottologVersion:
        return cls(commit=commit, describe=describe)

    def __str__(self) -> str:
        return f'Glottolog {self.describe} ({self.commit})'


def checkout_or_clone(tag_or_branch: str, *, target=None):
    if target is None:
        target = _languoids.get_repo_root()

    target = _tools.path_from_filename(target)

    if not target.exists():
        clone = git_clone(tag_or_branch, target=target)
    else:
        clone = None

    checkout = git_checkout(tag_or_branch, target=target)
    return clone, checkout


def git_clone(tag_or_branch: str, *, target,
              depth: int = 1):
    log.info('clone Glottolog master repo at %r into %r', tag_or_branch, target)
    cmd = ['git', 'clone',
           '-c', 'advice.detachedHead=false',
           '--single-branch',
           '--branch', tag_or_branch,
           '--depth', f'{depth:d}',
           REPO_URL, target]
    return _tools.run(cmd, check=True)


def git_checkout(tag_or_branch: str, *, target,
                 set_branch: str = __package__):
    log.info('checkout %r and (re)set branch %r', tag_or_branch, set_branch)
    cmd = ['git', 'checkout']
    if set_branch is not None:
        cmd += ['-B', set_branch]
    cmd.append(tag_or_branch)
    return _tools.run(cmd, cwd=target, check=True)


def git_rev_parse(repo_root, revision: str = 'HEAD',
                  *, verify: bool = True) -> str:
    log.info('get %r git_commit from %r', revision, repo_root)
    cmd = ['git', 'rev-parse']
    if verify:
        cmd.append('--verify')
    cmd.append(revision)
    commit = _tools.run(cmd, cwd=repo_root, check=True,
                        capture_output=True, unpack=True)
    log.info('git_commit: %r', commit)
    return commit


def git_describe(repo_root) -> str:
    log.info('get git_describe from %r', repo_root)
    cmd = ['git', 'describe', '--tags', '--always']
    describe = _tools.run(cmd, cwd=repo_root, check=True,
                          capture_output=True, unpack=True)
    log.info('git_describe: %r', describe)
    return describe


def git_status(repo_root) -> str:
    log.debug('get status from %r', repo_root)
    cmd = ['git', 'status', '--porcelain']
    return _tools.run(cmd, cwd=repo_root, check=True,
                      capture_output=True, unpack=True)


def git_status_is_clean(repo_root) -> bool:
    """Return if there are neither changes in the index nor untracked files."""
    log.info('get clean from %r', repo_root)
    status = git_status(repo_root)
    clean = not status
    log.info('clean: %r', clean)
    return clean
