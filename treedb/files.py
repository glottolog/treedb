# files.py - load/write ../../languoids/tree/**/md.ini

from __future__ import unicode_literals
from __future__ import print_function

import os
import logging
import configparser

from ._compat import pathlib

from ._compat import ENCODING, iteritems

from . import tools as _tools

from . import ROOT

__all__ = ['get_default_root', 'set_root', 'iterfiles', 'save', 'roundtrip']

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


def set_root(repo_root, treepath=TREE_IN_ROOT, resolve=False):
    """Set and return default root for glottolog lanugoid directory tree."""
    log.info('set_root')
    if repo_root is None:
        raise ValueError('missing repo_root path: %r' % repo_root)

    path = _tools.path_from_filename(repo_root)
    log.debug('repo root: %r', repo_root)
    if resolve:
        path = _tools.path_from_filename(path).resolve(strict=False)

    ROOT.path = repo_root / _tools.path_from_filename(treepath)
    log.debug('root: %r', ROOT)
    return ROOT


class ConfigParser(configparser.ConfigParser):
    """Conservative ConfigParser with encoding header."""

    _basename = BASENAME

    _header = '# -*- coding: %s -*-\n'

    _newline = '\r\n'

    _init_defaults = {
        'delimiters': ('=',),
        'comment_prefixes': ('#',),
        'interpolation': None,
    }

    @classmethod
    def from_filename(cls, filename, encoding=ENCODING, **kwargs):
        path = _tools.path_from_filename(filename)
        return cls.from_path(path, encoding=encoding, **kwargs)

    @classmethod
    def from_path(cls, path, encoding=ENCODING, **kwargs):
        assert path.name == cls._basename

        inst = cls(**kwargs)
        with path.open(encoding=encoding) as f:
            inst.read_file(f)
        return inst

    def __init__(self, defaults=None, **kwargs):
        for k, v in iteritems(self._init_defaults):
            kwargs.setdefault(k, v)
        super(ConfigParser, self).__init__(defaults=defaults, **kwargs)

    def to_filename(self, filename, encoding=ENCODING):
        path = _tools.path_from_filename(filename)
        self.to_path(path, encoding=encoding)

    def to_path(self, path, encoding=ENCODING):
        with path.open('w', encoding=encoding, newline=self._newline) as f:
            f.write(self._header % encoding)
            self.write(f)


def iterfiles(root=ROOT, load=ConfigParser.from_path, make_path=pathlib.Path):
    """Yield ((<path_part>, ...), DirEntry, <ConfigParser object>) triples."""
    root = _tools.path_from_filename(root).resolve()
    log.info('enter directory tree %r', root)

    path_slice = slice(len(root.parts), -1)
    for n, d in enumerate(_tools.iterfiles(root), 1):
        path = make_path(d.path)
        yield path.parts[path_slice], d, load(path)
        if not (n % 2500):
            log.debug('%d files loaded' % n)

    log.info('exit directory tree %r', root)


def save(pairs, root=ROOT, basename=BASENAME, assume_changed=False,
         verbose=True, load=ConfigParser.from_path):
    """Write ((<path_part>, ...), <dict of dicts>) pairs to root."""
    root = _tools.path_from_filename(root)
    log.info('write directory tree %r', root)

    files_written = 0
    for path_tuple, d in pairs:
        path = root.joinpath(*path_tuple + (basename,))
        cfg = load(path)

        # FIXME: missing sections and options
        drop_sections = set(cfg.sections()).difference(set(d) | {'core', 'sources'})
        changed = assume_changed or bool(drop_sections)
        for s in drop_sections:
            cfg.remove_section(s)

        for section, s in iteritems(d):
            if section != 'core':
                drop_options = set(cfg.options(section))
                if section == 'iso_retirement':
                    drop_options.discard('change_to')
                drop_options.difference_update(set(s))

                changed = changed or bool(drop_options)
                for o in drop_options:
                    cfg.remove_option(section, o)

            for option, value in iteritems(s):
                if cfg.get(section, option) != value:
                    changed = True
                    cfg.set(section, option, value)

        if changed:
            if verbose:
                print('write %r' % path)
            cfg.to_path(path)
            files_written += 1

    log.info('%d files written', files_written)
    return files_written


def roundtrip(root=ROOT, verbose=False):
    """Do a load/save cycle with all config files."""
    triples = iterfiles(root)

    def _iterpairs(triples):
        for path_tuple, _, cfg in triples:
            d = {s: dict(m) for s, m in iteritems(cfg) if s != 'DEFAULT'}
            yield path_tuple, d

    return save(_iterpairs(triples), root, assume_changed=True, verbose=verbose)
