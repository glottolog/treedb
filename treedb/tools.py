# tools.py - generic re-useable self-contained helpers

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
           'check_output',
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

    update_hash(result, file)

    if not raw:
        result = result.hexdigest()
    return result


def update_hash(hash_, file, *, chunksize=2**16):  # 64 kB
    with path_from_filename(file).open('rb') as f:
        read = functools.partial(f.read, chunksize)
        for chunk in iter(read, b''):
            hash_.update(chunk)


def check_output(args, *, cwd=None, encoding=ENCODING):
    log.debug('get stdout of %r', args)

    if platform.system() == 'Windows':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None

    out = subprocess.check_output(args, cwd=cwd, startupinfo=startupinfo)
    return out.decode(encoding).strip()


def write_csv(filename, rows, *, header=None,
              dialect=DIALECT, encoding=ENCODING):
    if hasattr(filename, 'hexdigest'):
        hash_ = filename
        with io.StringIO() as f:
            writerows = csv.writer(f, dialect=dialect).writerows
            if header is not None:
                writerows([header])

            for rows in iterslices(rows, 100):
                writerows(rows)
                data = f.getvalue()
                data = data.encode(encoding)
                hash_.update(data)
                # NOTE: f.truncate(0) would prepend zero-bytes
                f.seek(0)
                f.truncate()

        return None

    elif filename is None:
        with io.StringIO() as f:
            writerows = csv.writer(f, dialect=dialect).writerows
            if header is not None:
                writerows([header])

            writerows(rows)

            data = f.getvalue()

        return data.encode(encoding)

    else:
        filename = path_from_filename(filename)
        with open(filename, 'wt', encoding=encoding, newline='') as f:
            writerows = csv.writer(f, dialect=dialect).writerows
            if header is not None:
                writerows([header])

            writerows(rows)

        return path_from_filename(filename)
