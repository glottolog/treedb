# generic re-useable self-contained helpers

import builtins
import bz2
import functools
import gzip
import hashlib
import io
import itertools
import json
import logging
import lzma
import operator
import os
import pathlib
import platform
import subprocess
import sys
import typing
import warnings

from . import _compat

ENCODING = 'utf-8'

PROGRESS_AFTER = 5_000

SUFFIX_OPEN_MODULE = {'.bz2': bz2,
                      '.gz': gzip,
                      '.xz': lzma}

__all__ = ['next_count',
           'groupby_attrgetter',
           'walk_scandir',
           'get_open_module',
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


def walk_scandir(top, *,
                 verbose: bool = False,
                 sortkey=operator.attrgetter('name')) -> typing.Iterator[os.DirEntry]:
    """Yield os.DirEntry objects for all files under top."""
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

        with os.scandir(root) as dentries:
            dentries = sorted(dentries, key=sortkey)

        dirs = []
        for dentry in dentries:
            if dentry.is_dir():
                dirs.append(dentry.path)
            else:
                yield dentry

        stack.extend(reversed(dirs))


def pipe_json_lines(file, documents=None, *, raw=False,
                    delete_present=True, autocompress=True):
    kwargs = {'delete_present': delete_present, 'autocompress': autocompress}

    if documents is not None:
        lines = pipe_json('dump', documents) if not raw else documents
        return pipe_lines(file, lines, **kwargs)

    lines = pipe_lines(file, **kwargs)
    return pipe_json('load', lines) if not raw else lines


def pipe_json(mode, documents):
    codec = {'load': json.loads, 'dump': json.dumps}[mode]

    def itercodec(docs):
        for d in docs:
            yield codec(d)

    if mode == 'load':
        assert next(itercodec(['null'])) is None
    else:
        assert next(itercodec([None])) == 'null'
            
    return itercodec(documents)


def pipe_lines(file, lines=None,
               *, delete_present=False, autocompress=True):
    open_func, result, hashobj = get_open_result(file,
                                                 write=lines is not None,
                                                 delete_present=delete_present,
                                                 autocompress=autocompress)

    if lines is not None:
        with open_func() as f:
            if hashobj is not None:
                write_wrapped(hashobj, f, lines)
            else:
                write_lines(f, lines)

            if file is None:
                result = f.getvalue()
        return result

    def iterlines():
        with open_func() as f:
            yield from f

    return iterlines()


def write_wrapped(hashsum, f, lines, *, bufsize=1000):
    write_line = functools.partial(print, file=f)
    buf = f.buffer
    for lines in iterslices(lines, size=bufsize):
        for line in lines:
            write_line(line)
        hashsum.update(buf.getbuffer())
        # NOTE: f.truncate(0) would prepend zero-bytes
        f.seek(0)
        f.truncate()


def iterslices(iterable, *, size):
    iterable = iter(iterable)
    next_slice = functools.partial(itertools.islice, iterable, size)
    return iter(lambda: list(next_slice()), [])


def write_lines(file, lines):
    write_line = functools.partial(print, file=file)
    for line in lines:
        write_line(line)


def path_from_filename(filename, *args, expanduser=True):
    if hasattr(filename, 'open'):
        assert not args
        result = filename
    else:
        result = pathlib.Path(filename, *args)

    if expanduser:
        result = result.expanduser()
    return result


def get_open_result(file, *, write=False, 
                    delete_present=False, autocompress=False,
                    _encoding: str = 'utf-8'):
    open_kwargs = {'mode': 'wt' if write else 'rt',
                   'encoding': _encoding}
    textio_kwargs = {'write_through': True, 'encoding': _encoding}

    path = fobj = hashobj = None

    if file is None:
        if not write:
            raise TypeError('file cannot be Null for write=False')
        result = fobj = io.StringIO()
    elif file is sys.stdout:
        result = fobj = io.TextIOWrapper(sys.stdout.buffer, **textio_kwargs)
    elif hasattr(file, 'write'):
        result = fobj = hashobj = file
    elif hasattr(file, 'hexdigest'):
        if not write:
            raise TypeError('missing lines')
        result = hashobj = file
        fobj = io.TextIOWrapper(io.BytesIO(), **textio_kwargs)
    else:
        result = path = path_from_filename(file)

    if path is None:
        log.info('write lines into: %r', fobj)
        open_func = lambda: _compat.nullcontext(fobj)
    else:
        log.info('write lines: %r', path)
        open_module = get_open_module(path, autocompress=autocompress)
        open_func = functools.partial(open_module.open, path,
                                      **open_kwargs)

        if write and path.exists():
            if not delete_present:
                raise RuntimeError('refuse to delete_present file: {path!r}')
            warnings.warn(f'delete present file: {path!r}')
            path.unlink()

    assert result is not None

    return open_func, result, hashobj


def get_open_module(filepath, autocompress=False):
    file = path_from_filename(filepath)

    suffix = file.suffix.lower()
    if autocompress:
        result = SUFFIX_OPEN_MODULE.get(suffix, builtins)
    else:
        result = builtins
        if suffix in SUFFIX_OPEN_MODULE:
            warnings.warn(f'file {file!r} has suffix {suffix!r}'
                          ' but autocompress=False')
    return result


def sha256sum(file, *, raw=False, autocompress=True):
    file = path_from_filename(file)
    open_module = get_open_module(file, autocompress=autocompress)

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
    def fromlist(cls, keys, *, start_index=0):
        return cls((k, i) for i, k in enumerate(uniqued(keys), start=start_index))

    def __missing__(self, key):
        return self._missing

    def _sortkey(self, key):
        return self[key], key

    def sorted(self, keys):
        return sorted(keys, key=self._sortkey)

    def sorted_enumerate(self, keys, start=0):
        keyed = sorted((self[key], key) for key in keys)
        return ((i, key) for i, (_, key) in enumerate(keyed, start))
