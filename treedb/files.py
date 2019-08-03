# files.py - load/write ../../languoids/tree/**/md.ini

from __future__ import unicode_literals
from __future__ import print_function

import configparser

from ._compat import pathlib

from ._compat import ENCODING, iteritems

from . import tools as _tools

from . import ROOT

__all__ = ['iterfiles', 'save', 'roundtrip']

BASENAME = 'md.ini'


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
        path = pathlib.Path(filename)
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
        path = pathlib.Path(filename)
        self.to_path(path, encoding=encoding)

    def to_path(self, path, encoding=ENCODING):
        with path.open('w', encoding=encoding, newline=self._newline) as f:
            f.write(self._header % encoding)
            self.write(f)


def iterfiles(root=ROOT, load=ConfigParser.from_path):
    """Yield ((<path_part>, ...), DirEntry, <ConfigParser object>) triples."""
    root = _tools.path_from_filename(root)
    path_slice = slice(len(root.parts), -1)
    for d in _tools.iterfiles(root):
        path = pathlib.Path(d.path)
        yield path.parts[path_slice], d, load(path)


def save(pairs, root=ROOT, basename=BASENAME, assume_changed=False,
         verbose=True, load=ConfigParser.from_path):
    """Write ((<path_part>, ...), <dict of dicts>) pairs to root."""
    root = _tools.path_from_filename(root)
    files_written = 0
    for path_tuple, d in pairs:
        path = root.joinpath(*path_tuple) / basename
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

    return files_written


def roundtrip(root=ROOT, verbose=True):
    """Do a load/save cycle with all config files."""
    triples = iterfiles(root)

    def _iterpairs(triples):
        for path_tuple, _, cfg in triples:
            d = {s: dict(m) for s, m in iteritems(cfg) if s != 'DEFAULT'}
            yield path_tuple, d

    return save(_iterpairs(triples), root, assume_changed=True, verbose=verbose)
