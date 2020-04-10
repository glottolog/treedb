# glottolog.py - clone/checkout the Glottolog master repo

import logging

from . import tools as _tools

from . import ROOT

__all__ = ['checkout_or_clone']

REPO_URL = 'https://github.com/glottolog/glottolog.git'


log = logging.getLogger(__name__)


def checkout_or_clone(tag_or_branch, *, target=ROOT):
    target = _tools.path_from_filename(target)
    if not target.exists():
        return git_clone(tag_or_branch, target=target)

    return git_checkout(tag_or_branch, target=target)


def git_clone(tag_or_branch, *, target=ROOT, depth=1):
    log.info('clone Glottolog master repo at %r into %r', tag_or_branch, target)
    cmd = ['git', 'clone',
           '--branch', tag_or_branch,
           '--depth', f'{depth:d}',
            REPO_URL, target]
    return _tools.run(cmd, check=True)


def git_checkout(tag_or_branch, *, target=ROOT, set_branch=__package__):
    log.info('checkout %r and (re)set branch %r', tag_or_branch, set_branch)
    cmd = ['git', 'checkout']
    if set_branch is not None:
        cmd += ['-B', set_branch]
    cmd.append(tag_or_branch)
    return _tools.run(cmd, cwd=target, check=True)
