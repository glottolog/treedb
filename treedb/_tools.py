"""Generic re-useable self-contained helpers."""

import builtins
import bz2
import configparser
import contextlib
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
import types  # noqa: F401 (used in doctest)
import typing
import warnings

ENCODING = 'utf-8'

PROGRESS_AFTER = 5_000

SUFFIX_OPEN_MODULE = {'.bz2': bz2,
                      '.gz': gzip,
                      '.xz': lzma}

__all__ = ['uniqued',
           'next_count',
           'groupby_itemgetter', 'groupby_attrgetter',
           'islice_limit',
           'iterslices',
           'walk_scandir',
           'pipe_json_lines', 'pipe_json', 'pipe_lines',
           'get_open_module',
           'path_from_filename',
           'sha256sum',
           'run',
           'Ordering',
           'ConfigParser']


log = logging.getLogger(__name__)


def uniqued(iterable, /):
    """Return list of unique hashable elements preserving order.

    >>> uniqued('spamham')
    ['s', 'p', 'a', 'm', 'h']
    """
    seen = set()
    return [i for i in iterable if i not in seen and not seen.add(i)]


def next_count(*, start: int = 0, step: int = 1):
    """Return a callable returning descending ints.

    >>> nxt = next_count(start=1)

    >>> nxt()
    1

    >>> nxt()
    2
    """
    count = itertools.count(start, step)
    return functools.partial(next, count)


def groupby_itemgetter(*indexes):
    """

    >>> groupby_second = groupby_itemgetter(1)

    >>> people = [('Sir Robin', True),
    ...           ('Brian', False),
    ...           ('Sir Lancelot', True)]

    >>> {knight: [name for name, _ in grp] for knight, grp in groupby_second(people)}
    {True: ['Sir Lancelot'], False: ['Brian']}

    >>> people_sorted = sorted(people, key=operator.itemgetter(1))

    >>> {knight: [name for name, _ in grp] for knight, grp in groupby_second(people_sorted)}
    {False: ['Brian'], True: ['Sir Robin', 'Sir Lancelot']}
    """
    key = operator.itemgetter(*indexes)
    return functools.partial(itertools.groupby, key=key)


def groupby_attrgetter(*attrnames):
    """

    >>> groupby_knight = groupby_attrgetter('knight')

    >>> people = [types.SimpleNamespace(name='Sir Robin', knight=True),
    ...           types.SimpleNamespace(name='Brian', knight=False),
    ...           types.SimpleNamespace(name='Sir Lancelot', knight=True)]

    >>> {knight: [g.name for g in grp] for knight, grp in groupby_knight(people)}
    {True: ['Sir Lancelot'], False: ['Brian']}

    >>> people_sorted = sorted(people, key=operator.attrgetter('knight'))

    >>> {knight: [g.name for g in grp] for knight, grp in groupby_knight(people_sorted)}
    {False: ['Brian'], True: ['Sir Robin', 'Sir Lancelot']}
    """
    key = operator.attrgetter(*attrnames)
    return functools.partial(itertools.groupby, key=key)


def islice_limit(iterable, /, *,
                 limit: typing.Optional[int] = None,
                 offset: typing.Optional[int] = 0):
    """Return a slice from iterable applying limit and offset.

    >>> list(islice_limit('spam', limit=3))
    ['s', 'p', 'a']

    >>> list(islice_limit('spam', offset=3))
    ['m']

    >>> list(islice_limit('spam', offset=1, limit=2))
    ['p', 'a']

    >>> list(islice_limit('spam'))
    ['s', 'p', 'a', 'm']
    """
    if limit is not None and offset:
        stop = offset + limit
        return itertools.islice(iterable, offset, stop)
    elif limit is not None:
        return itertools.islice(iterable, limit)
    elif offset:
        return itertools.islice(iterable, offset, None)
    return iterable


def iterslices(iterable, /, *, size: int):
    """Yield iterable in chunks of maximal size.

    >>> [tuple(chunk) for chunk in iterslices('bacon', size=2)]
    [('b', 'a'), ('c', 'o'), ('n',)]
    """
    iterable = iter(iterable)
    next_slice = functools.partial(itertools.islice, iterable, size)
    return iter(lambda: list(next_slice()), [])


def walk_scandir(top, /, *,
                 verbose: bool = False,
                 sortkey=operator.attrgetter('name')) -> typing.Iterator[os.DirEntry]:
    """Yield os.DirEntry objects for all files under top."""
    # NOTE: os.walk() ignores errors and this can be more efficient
    top = path_from_filename(top)
    if not top.is_absolute():
        top = pathlib.Path.cwd().joinpath(top).resolve()

    if platform.system() == 'Windows':
        top = pathlib.Path(fr'\\?\{top}')

    log.debug('recursive depth-first scandir on %r', top)
    log.debug('sortkey: %r', sortkey)

    stack = [str(top)]

    while stack:
        root = stack.pop()
        if verbose:  # pragma: no cover
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


def pipe_json_lines(file, documents=None, /, *,
                    delete_present: bool = True,
                    autocompress: bool = True,
                    newline: typing.Optional[str] = '\n',
                    sort_keys: bool = True,
                    compact: bool = True,
                    indent: typing.Optional[int] = None,
                    ensure_ascii: bool = False):
    r"""Load/dump json lines as endpoint pipe.

    >>> with io.StringIO() as f:
    ...     buf, nwritten = pipe_json_lines(f, [None, {'spam': None}])
    ...     json_lines = f.getvalue()

    >>> assert buf is f

    >>> nwritten
    2

    >>> json_lines
    'null\n{"spam":null}\n'

    >>> with io.StringIO(json_lines) as f:
    ...     documents = list(pipe_json_lines(f))
    >>> documents
    [None, {'spam': None}]
    """
    lines_kwargs = {'delete_present': delete_present,
                    'autocompress': autocompress,
                    'newline': newline}
    json_kwargs = {'sort_keys': sort_keys,
                   'ensure_ascii': ensure_ascii,
                   'compact': compact,
                   'indent': indent}

    if documents is not None:
        lines = pipe_json(documents, dump=True, **json_kwargs)
        return pipe_lines(file, lines, **lines_kwargs)

    lines = pipe_lines(file, **lines_kwargs)
    return pipe_json(lines, dump=False, **json_kwargs)


def pipe_json(documents, /, *, dump: bool,
              sort_keys: bool = True,
              compact: bool = False,
              indent: typing.Optional[int] = None,
              ensure_ascii: bool = False):
    """Bidirectional codec between a generator and a consumer."""
    codec = json.dumps if dump else json.loads

    if dump:
        dump_kwargs = {'sort_keys': sort_keys,
                       'indent': indent,
                       'ensure_ascii': ensure_ascii,
                       # json-serialize datetime.datetime
                       'default': operator.methodcaller('isoformat')}
        if compact:
            if indent:  # pragma: no cover
                warnings.warn(f'{indent=!r} overridden by {compact=}')
            dump_kwargs.update(indent=None,
                               separators=(',', ':'))
        elif indent:
            warnings.warn('non-canonical JSON Lines format from indent %r' % indent)

        codec = functools.partial(codec, **dump_kwargs)

    def itercodec(docs):
        return map(codec, docs)

    if dump:
        assert next(itercodec([None])) == 'null'
    else:
        assert next(itercodec(['null'])) is None

    return itercodec(documents)


def pipe_lines(file, lines=None, /, *, newline: typing.Optional[str] = None,
               delete_present: bool = False, autocompress: bool = True):
    open_func, result, hashobj = get_open_result(file,
                                                 write=lines is not None,
                                                 delete_present=delete_present,
                                                 autocompress=autocompress,
                                                 newline=newline)

    if lines is not None:
        with open_func() as f:
            if hashobj is not None:
                total = write_wrapped(hashobj, f, lines)
            else:
                total = write_lines(f, lines)

            if file is None:
                result = f.getvalue()
        return result, total

    def iterlines():
        with open_func() as f:
            yield from f

    return iterlines()


def write_wrapped(hashsum, f, lines, /, *, buflines: int = 1_000):
    write_line = functools.partial(print, file=f)
    buf = f.buffer
    total = 0
    for lines in iterslices(lines, size=buflines):
        for line in lines:
            write_line(line)
        total += len(lines)
        hashsum.update(buf.getbuffer())
        # NOTE: f.truncate(0) would prepend zero-bytes
        f.seek(0)
        f.truncate()
    return total


def write_lines(file, lines, /):
    r"""

    >>> with io.StringIO() as f:
    ...    write_lines(f, ['spam', 'eggs'])
    ...    text = f.getvalue()
    2

    >>> text
    'spam\neggs\n'
    """
    write_line = functools.partial(print, file=file)
    total = 0
    for total, line in enumerate(lines, start=1):
        write_line(line)
    return total


def path_from_filename(filename, /, *args, expanduser: bool = True):
    if hasattr(filename, 'open'):
        assert not args
        result = filename
    else:
        result = pathlib.Path(filename, *args)

    if expanduser:
        result = result.expanduser()
    return result


def get_open_result(file, /, *, write: bool = False,
                    delete_present: bool = False, autocompress: bool = False,
                    newline: typing.Optional[str] = None,
                    _encoding: str = 'utf-8'):
    open_kwargs = {'mode': 'wt' if write else 'rt',
                   'encoding': _encoding,
                   'newline': newline}
    textio_kwargs = {'write_through': True, 'encoding': _encoding}

    path = fobj = hashobj = None

    if file is None:
        if not write:  # pragma: no cover
            raise TypeError('file cannot be Null for write=False')
        result = fobj = io.StringIO()
    elif file is sys.stdout:
        result = fobj = io.TextIOWrapper(sys.stdout.buffer,
                                         newline=open_kwargs.pop('newline', None),
                                         **textio_kwargs)
    elif hasattr(file, 'write'):
        result = fobj = file
    elif hasattr(file, 'hexdigest'):
        if not write:  # pragma: no cover
            raise TypeError('missing lines')
        result = hashobj = file
        fobj = io.TextIOWrapper(io.BytesIO(),
                                newline=open_kwargs.pop('newline', None),
                                **textio_kwargs)
    else:
        result = path = path_from_filename(file)

    if path is None:
        log.info('write lines into: %r', fobj)

        def open_func():
            return contextlib.nullcontext(fobj)
    else:
        log.info('write lines: %r', path)
        open_module = get_open_module(path, autocompress=autocompress)
        open_func = functools.partial(open_module.open, path,
                                      **open_kwargs)

        if write and path.exists():
            if not delete_present:  # pragma: no cover
                raise RuntimeError('refuse to delete_present file: {path!r}')
            warnings.warn(f'delete present file: {path!r}')
            path.unlink()

    assert result is not None

    return open_func, result, hashobj


def get_open_module(filepath, /, *, autocompress: bool = False):
    file = path_from_filename(filepath)

    suffix = file.suffix.lower()
    if autocompress:
        result = SUFFIX_OPEN_MODULE.get(suffix, builtins)
    else:
        result = builtins
        if suffix in SUFFIX_OPEN_MODULE:  # pragma: no cover
            warnings.warn(f'file {file!r} has suffix {suffix!r}'
                          ' but autocompress=False')
    return result


def sha256sum(file, /, *, raw: bool = False, autocompress: bool = True,
              hash_file_string: bool = False,
              file_string_encoding: str = ENCODING):
    """

    >>> sha256sum('', hash_file_string=True)
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    """
    hashobj = hashlib.sha256()

    if hash_file_string:
        encoded_bytes = file.encode(file_string_encoding)
        hashobj.update(encoded_bytes)
    else:
        file = path_from_filename(file)
        open_module = get_open_module(file, autocompress=autocompress)

        with open_module.open(file, 'rb') as f:
            update_hashobj(hashobj, f)

    return hashobj if raw else hashobj.hexdigest()


def update_hashobj(hashobj, file, /, *, chunksize: int = 2**16):  # 64 KiB
    for chunk in iter(functools.partial(file.read, chunksize), b''):
        hashobj.update(chunk)


def run(cmd, /, *, capture_output: bool = False,
        unpack: bool = False, cwd=None, check: bool = False,
        encoding: str = ENCODING):
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


class Ordering(dict):
    """Dict returning infinity for unknown keys ordering them lexicographically.

    >>> o = Ordering.fromlist(['spam', 'eggs', 'bacon'])

    >>> o
    {'spam': 0, 'eggs': 1, 'bacon': 2}

    >>> o['ham']
    inf

    >>> o.sorted(['ham', 'bacon', 'eggs', 'am'])
    ['eggs', 'bacon', 'am', 'ham']

    >>> list(o.sorted_enumerate(['ham', 'bacon', 'eggs'], start=1))
    [(1, 'eggs'), (2, 'bacon'), (3, 'ham')]
    """

    _missing = float('inf')

    @classmethod
    def fromlist(cls, keys, /, *, start_index: int = 0):
        return cls((k, i) for i, k in enumerate(uniqued(keys), start=start_index))

    def __missing__(self, key, /):
        return self._missing

    def _sortkey(self, key, /):
        return self[key], key

    def sorted(self, keys, /):
        return sorted(keys, key=self._sortkey)

    def sorted_enumerate(self, keys, /, *, start: int = 0):
        keyed = sorted((self[key], key) for key in keys)
        return ((i, key) for i, (_, key) in enumerate(keyed, start=start))


class ConfigParser(configparser.ConfigParser):
    """Conservative ConfigParser with optional encoding header."""

    _basename = None

    _init_defaults = {'delimiters': ('=',),
                      'comment_prefixes': ('#',),
                      'interpolation': None}

    _newline = None

    _header = None

    @classmethod
    def from_file(cls, filename, /, *, encoding=ENCODING, **kwargs):
        path = path_from_filename(filename)
        if cls._basename is not None and path.name != cls._basename:
            raise RuntimeError(f'unexpected filename {path!r}'
                               f' (must end with {cls._basename})')

        inst = cls(**kwargs)
        with path.open(encoding=encoding) as f:
            inst.read_file(f)
        return inst

    def __init__(self, /, *, defaults=None, **kwargs):
        for k, v in self._init_defaults.items():
            kwargs.setdefault(k, v)
        super().__init__(defaults=defaults, **kwargs)

    def to_dict(self, /, *, sort_sections: bool = False,
                _default_section: str = configparser.DEFAULTSECT):
        items = sorted(self.items()) if sort_sections else self.items()
        return {name: dict(section) for name, section in items
                if name != _default_section}

    def to_file(self, filename, /, *, encoding=ENCODING):
        path = path_from_filename(filename)
        with path.open('wt', encoding=encoding, newline=self._newline) as f:
            if self._header is not None:
                f.write(self._header.format(encoding=encoding))
            self.write(f)
