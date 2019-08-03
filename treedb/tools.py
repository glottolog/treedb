# tools.py - generic re-useable self-contained helpers

from __future__ import print_function

import csv
import hashlib
import platform
import operator
import itertools
import functools
import subprocess

from ._compat import pathlib
from ._compat import scandir

from . import _compat

from ._compat import ENCODING, DIALECT

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
    top = path_from_filename(top)
    if not top.is_absolute():
        top = pathlib.Path.cwd().join(top).resolve()

    stack = [str(top)]
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


def sha256sum(file, raw=False):
    result = hashlib.sha256()
    update_hash(result, file)
    if not raw:
        result = result.hexdigest()
    return result


def update_hash(hash_, file, chunksize=2**16):  # 64 kB
    with path_from_filename(file).open('rb') as f:
        read = functools.partial(f.read, chunksize)
        for chunk in iter(read, b''):
            hash_.update(chunk)


def check_output(args, cwd=None, encoding=ENCODING):
    if platform.system() == 'Windows':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None

    out = subprocess.check_output(args, cwd=cwd, startupinfo=startupinfo)
    return out.decode(encoding).strip()


def write_csv(filename, rows, header=None, dialect=DIALECT, encoding=ENCODING):
    make_writer = functools.partial(csv.writer, dialect=dialect)

    if hasattr(filename, 'hexdigest'):
        hash_ = filename
        with _compat.make_csv_io() as f:
            writer = make_writer(f)
            write = functools.partial(_compat.csv_write, writer, encoding=encoding)
            get_bytes = functools.partial(_compat.get_csv_io_bytes, encoding=encoding)
            write([], header=header)
            for rows in iterslices(rows, 500):
                write(rows)
                data = get_bytes(f.getvalue())
                hash_.update(data)
                # NOTE: f.truncate(0) would prepend zero-bytes
                f.seek(0)
                f.truncate()
        return None
    elif filename is None:
        with _compat.make_csv_io() as f:
            writer = make_writer(f)
            _compat.csv_write(writer, rows, header=header, encoding=encoding)
            data = f.getvalue()
        return _compat.get_csv_io_bytes(data, encoding)

    with _compat.csv_open(filename, 'w', encoding=encoding) as f:
        writer = make_writer(f)
        _compat.csv_write(writer, rows, header=header, encoding=encoding)

    return path_from_filename(filename)
