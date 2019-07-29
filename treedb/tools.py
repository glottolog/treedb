# tools.py - generic re-useable self-contained helpers

import io
import hashlib
import platform
import operator
import itertools
import functools
import subprocess

from ._compat import pathlib
from ._compat import scandir

from . import ENCODING

__all__ = [
    'groupby_attrgetter',
    'iterfiles',
    'sha256sum'
]


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


def sha256sum(file, chunksize=2**16):  # 64 kB
    result = hashlib.sha256()
    with io.open(file, 'rb') as f:
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
