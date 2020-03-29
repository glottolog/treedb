# files.py - load/write ../../languoids/tree/**/md.ini

import configparser
import logging
import os

from . import (tools as _tools,
               fields as _fields)

from . import ROOT

__all__ = ['get_default_root', 'set_root',
           'iterfiles',
           'write_files', 'roundtrip']

TREE_IN_ROOT = _tools.path_from_filename('languoids', 'tree')

BASENAME = 'md.ini'


log = logging.getLogger(__name__)


def get_default_root(env_var, checkout_root, package_root):
    """Return default root from environment variable or fallbacks."""
    result = os.getenv(env_var)
    if result is None:
        from . import __file__
        pkg_dir = _tools.path_from_filename(__file__).parent
        if (pkg_dir.parent / '.git').exists():
            result = checkout_root
        else:
            result = package_root
    return result


def set_root(repo_root, treepath=TREE_IN_ROOT, *, resolve=False):
    """Set and return default root for glottolog lanugoid directory tree."""
    log.info('set_root')
    if repo_root is None:
        raise ValueError(f'missing repo_root path: {repo_root!r}')

    log.debug('repo root: %r', repo_root)
    repo_path = _tools.path_from_filename(repo_root)
    if resolve:
        repo_path = repo_path.resolve(strict=False)

    ROOT.path = repo_path / _tools.path_from_filename(treepath)
    log.debug('root: %r', ROOT)
    return ROOT


class ConfigParser(configparser.ConfigParser):
    """Conservative ConfigParser with encoding header."""

    _basename = BASENAME

    _header = '# -*- coding: {encoding} -*-\n'

    _newline = '\r\n'

    _init_defaults = {
        'delimiters': ('=',),
        'comment_prefixes': ('#',),
        'interpolation': None,
    }

    @classmethod
    def from_file(cls, filename, *, encoding=_tools.ENCODING, **kwargs):
        path = _tools.path_from_filename(filename)
        if path.name != cls._basename:
            raise RuntimeError(f'unexpected filename {path!r}'
                               f' (must end with {cls._basename})')

        inst = cls(**kwargs)
        with path.open(encoding=encoding) as f:
            inst.read_file(f)
        return inst

    def __init__(self, *, defaults=None, **kwargs):
        for k, v in self._init_defaults.items():
            kwargs.setdefault(k, v)
        super().__init__(defaults=defaults, **kwargs)

    def to_file(self, filename, *, encoding=_tools.ENCODING):
        path = _tools.path_from_filename(filename)
        with path.open('wt', encoding=encoding, newline=self._newline) as f:
            f.write(self._header.format(encoding=encoding))
            self.write(f)


def iterfiles(root=ROOT, *, progress_after=_tools.PROGRESS_AFTER):
    """Yield ((<path_part>, ...), DirEntry, <ConfigParser object>) triples."""
    make_path = _tools.path_from_filename
    load_config = ConfigParser.from_file

    root = make_path(root).resolve()
    log.info(f'start parsing {BASENAME} files from %r', root)

    path_slice = slice(len(root.parts), -1)

    msg = f'%s {BASENAME} files parsed'

    n = 0
    for n, d in enumerate(_tools.iterfiles(root), 1):
        path = make_path(d)
        yield path.parts[path_slice], d, load_config(path)

        if not (n % progress_after):
            log.info(msg, f'{n:_d}')

    log.info(f'%s {BASENAME} files total', f'{n:_d}')


def write_files(records, *, root=ROOT, assume_changed=False, verbose=True,
                basename=BASENAME, is_lines = _fields.Fields.is_lines):
    """Write ((<path_part>, ...), <dict of dicts>) pairs to root."""
    load_config = ConfigParser.from_file

    def iterpairs(records):
        for p, r in records:
            for section, s in r.items():
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield p, r

    root = _tools.path_from_filename(root)
    log.info('write directory tree %r', root)

    files_written = 0
    for path_tuple, d in iterpairs(records):
        path = root.joinpath(*path_tuple + (basename,))
        cfg = load_config(path)

        # FIXME: missing sections and options
        drop_sections = set(cfg.sections()).difference(set(d) | {'core', 'sources'})
        changed = assume_changed or bool(drop_sections)
        for s in drop_sections:
            cfg.remove_section(s)

        for section, s in d.items():
            if section != 'core':
                drop_options = set(cfg.options(section))
                if section == 'iso_retirement':
                    drop_options.discard('change_to')
                drop_options.difference_update(set(s))

                changed = changed or bool(drop_options)
                for o in drop_options:
                    cfg.remove_option(section, o)

            for option, value in s.items():
                if cfg.get(section, option) != value:
                    changed = True
                    cfg.set(section, option, value)

        if changed:
            if verbose:
                print(f'write {path!r}')
            cfg.to_file(path)
            files_written += 1

    log.info('%d files written', files_written)
    return files_written


def roundtrip(root=ROOT, *, verbose=False):
    """Do a load/save cycle with all config files."""
    triples = iterfiles(root)

    def _iterpairs(triples):
        for path_tuple, _, cfg in triples:
            d = {s: dict(m) for s, m in cfg.items() if s != 'DEFAULT'}
            yield path_tuple, d

    pairs = _iterpairs(triples)
    return write_files(pairs, root, assume_changed=True, verbose=verbose)
