# _tools.py - generic re-useable self-contained helpers

import builtins
import bz2
import functools
import gzip
import hashlib
import itertools
import logging
import lzma
import operator
import os
import pathlib
import platform
import subprocess
import warnings

ENCODING = 'utf-8'

PROGRESS_AFTER = 5_000

SUFFIX_OPEN_MODULE = {'.bz2': bz2,
                      '.gz': gzip,
                      '.xz': lzma}

__all__ = ['next_count',
           'groupby_attrgetter',
           'iterfiles',
           'path_from_filename',
           'sha256sum',
           'run',
           'Ordering']


log = logging.getLogger(__name__)


def next_count(start=0, step=1):
    count = itertools.count(start, step)
    return functools.partial(next, count)


def groupby_attrgetter(*attrnames):
    key = operator.attrgetter(*attrnames)
    return functools.partial(itertools.groupby, key=key)


def iterfiles(top, *, verbose=False, sortkey=operator.attrgetter('name')):
    """Yield DirEntry objects for all files under top."""
    # NOTE: os.walk() ignores errors and this can be more efficient
    top = path_from_filename(top)
    if not top.is_absolute():
        top = pathlib.Path.cwd().joinpath(top).resolve()

    if platform.platform().startswith('Windows-'):
        top = pathlib.Path(fr'\\?\{top}')

    log.debug('recursive depth-first scandir on %r', top)
    log.debug('sortkey: %r', sortkey)

    stack = [str(top)]

    while stack:
        root = stack.pop()
        if verbose:
            print(root)

        dentries = sorted(os.scandir(root), key=sortkey)

        dirs = []
        for d in dentries:
            if d.is_dir():
                dirs.append(d.path)
            else:
                yield d

        stack.extend(reversed(dirs))


def path_from_filename(filename, *args, expanduser=True):
    if hasattr(filename, 'open'):
        assert not args
        result = filename
    else:
        result = pathlib.Path(filename, *args)

    if expanduser:
        result = result.expanduser()
    return result


def sha256sum(file, *, raw=False, autocompress=True):
    file = path_from_filename(file)

    suffix = ''.join(str(file).rpartition('.')[1:]).lower()
    if autocompress:
        open_module = SUFFIX_OPEN_MODULE.get(suffix, builtins)
    else:
        open_module = builtins
        if suffix in SUFFIX_OPEN_MODULE:
            warnings.warn(f'suffix {suffix!r} but autocompress=False')

    result = hashlib.sha256()

    with open_module.open(file, 'rb') as f:
        update_hash(result, f)

    if not raw:
        result = result.hexdigest()
    return result


def update_hash(hash_, file, *, chunksize=2**16):  # 64 KiB
    for chunk in iter(functools.partial(file.read, chunksize), b''):
        hash_.update(chunk)


def run(cmd, *, capture_output=False, unpack=False, cwd=None, check=False,
        encoding=ENCODING):
    log.info('subprocess.run(%r)', cmd)

    if platform.system() == 'Windows':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None

    kwargs = {'cwd': cwd, 'encoding': encoding, 'startupinfo': startupinfo}

    if capture_output:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, **kwargs)
        out, err = proc.communicate()
        if check and proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd,
                                                output=out, stderr=err)
        proc = subprocess.CompletedProcess(cmd, proc.returncode, out, err)
        if unpack:
            return proc.stdout.strip()
    else:
        proc = subprocess.run(cmd, check=check, **kwargs)
    return proc


def uniqued(iterable):
    seen = set()
    return [i for i in iterable if i not in seen or not seen.add(i)]


class Ordering(dict):

    _missing = float('inf')

    @classmethod
    def fromlist(cls, keys):
        return cls((k, i) for i, k in enumerate(uniqued(keys)))

    def __missing__(self, key):
        return self._missing

    def _sortkey(self, key):
        return self[key], key

    def sorted(self, keys):
        return sorted(keys, key=self._sortkey)