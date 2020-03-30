# tools.py - generic re-useable self-contained helpers

import functools
import hashlib
import itertools
import logging
import operator
import os
import pathlib
import platform
import subprocess

ENCODING = 'utf-8'

PROGRESS_AFTER = 2_500

__all__ = ['next_count',
           'groupby_itemgetter', 'groupby_attrgetter',
           'iterfiles',
           'path_from_filename',
           'sha256sum',
           'run',
           'Ordering']


log = logging.getLogger(__name__)


def next_count(start=0, step=1):
    count = itertools.count(start, step)
    return functools.partial(next, count)


def groupby_itemgetter(*items):
    key = operator.itemgetter(*items)
    return functools.partial(itertools.groupby, key=key)


def groupby_attrgetter(*attrnames):
    key = operator.attrgetter(*attrnames)
    return functools.partial(itertools.groupby, key=key)


def iterfiles(top, *, verbose=False):
    """Yield DirEntry objects for all files under top."""
    # NOTE: os.walk() ignores errors and this can be more efficient
    top = path_from_filename(top)
    if not top.is_absolute():
        top = pathlib.Path.cwd().joinpath(top).resolve()
    log.debug('recursive scandir %r', top)

    stack = [str(top)]

    while stack:
        root = stack.pop()
        if verbose:
            print(root)
        direntries = os.scandir(root)
        dirs = []
        for d in direntries:
            if d.is_dir():
                dirs.append(d.path)
            else:
                yield d
        stack.extend(dirs[::-1])


def path_from_filename(filename, *args, expanduser=True):
    if hasattr(filename, 'open'):
        assert not args
        result = filename
    else:
        result = pathlib.Path(filename, *args)

    if expanduser:
        result = result.expanduser()
    return result


def sha256sum(file, *, raw=False):
    result = hashlib.sha256()

    with path_from_filename(file).open('rb') as f:
        update_hash(result, f)

    if not raw:
        result = result.hexdigest()
    return result


def update_hash(hash_, file, *, chunksize=2**16):  # 64 kB
    read = functools.partial(file.read, chunksize)
    for chunk in iter(read, b''):
        hash_.update(chunk)


def run(cmd, *, capture_output=False, cwd=None, encoding=ENCODING, unpack=False):
    log.info('subprocess.run(%r)', cmd)

    kwargs = {'capture_output': capture_output,
              'cwd': cwd, 'encoding': encoding}

    if platform.system() == 'Windows':
        kwargs['startupinfo'] = s = subprocess.STARTUPINFO()
        s.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        s.wShowWindow = subprocess.SW_HIDE
    else:
        kwargs['startupinfo'] = None

    proc = subprocess.run(cmd, **kwargs)

    if capture_output and unpack:
        return proc.stdout.strip()
    return proc


class Ordering(dict):

    _missing = float('inf')

    @classmethod
    def fromlist(cls, keys):
        seen = set()
        uniqued = [k for k in keys if k not in seen or not seen.add(k)]
        return cls((k, i) for i, k in enumerate(uniqued))

    def __missing__(self, key):
        return self._missing

    def _sortkey(self, key):
        return self[key], key

    def sorted(self, keys):
        return sorted(keys, key=self._sortkey)
