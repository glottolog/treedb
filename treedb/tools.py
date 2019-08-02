# tools.py - generic re-useable self-contained helpers

from __future__ import print_function

import io
import csv
import hashlib
import datetime
import platform
import operator
import itertools
import functools
import subprocess

from ._compat import pathlib
from ._compat import scandir

from . import _compat

from ._compat import ENCODING

__all__ = [
    'next_count',
    'iterslices',
    'groupby_itemgetter', 'groupby_attrgetter',
    'iterfiles',
    'path_from_filename',
    'sha256sum',
    'check_output',
    'write_csv',
]


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


def iterfiles(top, verbose=False):
    """Yield DirEntry objects for all files under top."""
    # NOTE: os.walk() ignores errors and this can be more efficient
    if isinstance(top, pathlib.Path):
        top = str(top)
    stack = [top]
    while stack:
        root = stack.pop()
        if verbose:
            print(root)
        direntries = scandir(root)
        dirs = []
        for d in direntries:
            if d.is_dir():
                dirs.append(d.path)
            else:
                yield d
        stack.extend(dirs[::-1])


def path_from_filename(filename, *args):
    if isinstance(filename, pathlib.Path):
        assert not args
        del args
    else:
        filename = pathlib.Path(filename, *args)
    return filename


def sha256sum(file, chunksize=2**16):  # 64 kB
    result = hashlib.sha256()
    file = path_from_filename(file)
    with file.open('rb') as f:
        read = functools.partial(f.read, chunksize)
        for chunk in iter(read, b''):
            result.update(chunk)
    return result


def check_output(args, cwd=None, encoding=ENCODING):
    if platform.system() == 'Windows':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None

    out = subprocess.check_output(args, cwd=cwd, startupinfo=startupinfo)
    return out.decode(encoding).strip()


def write_csv(filename, rows, header=None, encoding=ENCODING, dialect='excel'):
    if filename is None:
        with _compat.make_csv_io() as f:
            writer = csv.writer(f, dialect=dialect)
            _compat.csv_write(writer, rows, header=header, encoding=encoding)
            data = f.getvalue()
        return _compat.get_csv_io_bytes(data, encoding)

    with _compat.csv_open(filename, 'w', encoding=encoding) as f:
        writer = csv.writer(f, dialect=dialect)
        _compat.csv_write(writer, rows, header=header, encoding=encoding)

    return path_from_filename(filename)
