# tools.py - generic re-useable self-contained helpers

import contextlib
import csv
import functools
import hashlib
import io
import itertools
import logging
import operator
import os
import pathlib
import platform
import subprocess

DIALECT = 'excel'

ENCODING = 'utf-8'

__all__ = ['next_count',
           'iterslices',
           'groupby_itemgetter', 'groupby_attrgetter',
           'iterfiles',
           'path_from_filename',
           'sha256sum',
           'run',
           'write_csv']


log = logging.getLogger(__name__)


def next_count(start=0, step=1):
    count = itertools.count(start, step)
    return functools.partial(next, count)


def iterslices(iterable, size):
    iterable = iter(iterable)
    next_slice = functools.partial(itertools.islice, iterable, size)
    return iter(lambda: list(next_slice()), [])


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


def write_csv(filename, rows, *, header=None, dialect=DIALECT,
              encoding=ENCODING):
    open_kwargs = {'encoding': encoding, 'newline': ''}
    textio_kwargs = dict(write_through=True, **open_kwargs)

    if filename is None:
        if encoding is None:
            f = io.StringIO()
        else:
            f = io.TextIOWrapper(io.BytesIO(), **textio_kwargs)
    elif hasattr(filename, 'write'):
        result = filename
        if encoding is None:
            f = filename
        else:
            f = io.TextIOWrapper(filename, **textio_kwargs)
        f = contextlib.nullcontext(f)
    elif hasattr(filename, 'hexdigest'):
        result = filename
        assert encoding is not None
        f = io.TextIOWrapper(io.BytesIO(), **textio_kwargs)
        hash_ = filename
    else:
        result = path_from_filename(filename)
        assert encoding is not None
        f = open(filename, 'wt', **open_kwargs)

    with f as f:
        writer = csv.writer(f, dialect=dialect)

        if header is not None:
            writer.writerows([header])

        if hasattr(filename, 'hexdigest'):
            buf = f.buffer
            for rows in iterslices(rows, 1_000):
                writer.writerows(rows)
                hash_.update(buf.getbuffer())
                # NOTE: f.truncate(0) would prepend zero-bytes
                f.seek(0)
                f.truncate()
        else:
            writer.writerows(rows)

        if filename is None:
            if encoding is not None:
                f = f.buffer
            result = f.getvalue()

    if hasattr(filename, 'write') and encoding is not None:
        f.detach()

    return result
