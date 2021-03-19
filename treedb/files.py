# files.py - load/write ../../languoids/tree/**/md.ini

import configparser
import logging

from . import ROOT

from . import _tools
from . import fields as _fields

__all__ = ['set_root', 'get_repo_root',
           'iterfiles',
           'write_files', 'roundtrip']

TREE_IN_ROOT = _tools.path_from_filename('languoids', 'tree')

BASENAME = 'md.ini'


log = logging.getLogger(__name__)


def set_root(repo_root, treepath=TREE_IN_ROOT, *, resolve=False):
    """Set and return default root for glottolog lanugoid directory tree."""
    log.info('set_root: %r', repo_root)
    if repo_root is None:
        raise ValueError(f'missing repo_root path: {repo_root!r}')

    repo_path = _tools.path_from_filename(repo_root)
    if resolve:
        repo_path = repo_path.resolve(strict=False)

    ROOT.path = repo_path / _tools.path_from_filename(treepath)
    return ROOT


def get_repo_root(root, treepath=TREE_IN_ROOT):
    repo_root = _tools.path_from_filename(root)
    for _ in treepath.parts:
        repo_root = repo_root.parent
    return repo_root


class ConfigParser(configparser.ConfigParser):
    """Conservative ConfigParser with encoding header."""

    _basename = BASENAME

    _header = '# -*- coding: {encoding} -*-\n'

    _newline = '\r\n'

    _init_defaults = {'delimiters': ('=',),
                      'comment_prefixes': ('#',),
                      'interpolation': None}

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


def records_from_files(triples):
    for path_tuple, _, cfg in triples:
        d = {s: dict(m) for s, m in cfg.items() if s != 'DEFAULT'}
        yield path_tuple, d


def roundtrip(root=ROOT, *, verbose=False,
              progress_after=_tools.PROGRESS_AFTER):
    """Do a load/save cycle with all config files."""
    triples = iterfiles(root,
                        progress_after=progress_after)
                        
    records = records_from_files(triples)
    return write_files(records, root,
                       replace=False,
                       progress_after=progress_after)


def write_files(records, *, root=ROOT, replace=False,
                progress_after=_tools.PROGRESS_AFTER, basename=BASENAME):
    """Write ((<path_part>, ...), <dict of dicts>) pairs to root."""
    load_config = ConfigParser.from_file

    def iterpairs(records, is_lines=_fields.is_lines):
        for p, r in records:
            for section, s in r.items():
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield p, r

    root = _tools.path_from_filename(root)
    log.info(f'start writing {basename} files into %r', root)

    if replace:
        log.warning(f'replace present {basename} files')

    sorted_sections = _fields.sorted_sections
    sorted_options = _fields.sorted_options
    is_lines = _fields.is_lines

    core_sections = {'core'}
    leave_empty = {'sources'}

    files_written = 0
    for path_tuple, d in iterpairs(records):
        path = root.joinpath(*path_tuple + (basename,))

        cfg = load_config(path)

        if replace:
            cfg.clear()

        changed = False

        old_sections = set(cfg.sections())
        old_empty = {s for s in old_sections if not any(v for _, v in cfg.items(s))}

        new_sections = {sec for sec, s in d.items() if s}
        leave = (old_empty - new_sections) & leave_empty

        drop = old_sections - new_sections - leave - core_sections
        if drop:
            drop = sorted_sections(drop)
            log.debug('cfg.remove_section(s) for s in %r', drop)
            for s in drop:
                cfg.remove_section(s)

            changed = True

        add = new_sections - old_sections
        if add:
            add = sorted_sections(add)
            if not replace:
                log.debug('cfg.add_section(s) for s in %r', add)
            for s in add:
                cfg.add_section(s)

            changed = True

        for section in sorted_sections(d):
            s = d[section]
            if section not in old_sections or section in core_sections:
                pass
            elif section in leave:
                continue
            else:
                old_options = set(cfg.options(section))
                new_options = {k for k, v in s.items() if v}
                drop_options = old_options - new_options
                if section == 'iso_retirement':
                    drop_options.discard('change_to')

                if drop_options:
                    drop_options = sorted_options(section, drop_options)
                    log.debug('cfg.remove_option(%r, o) for o in %r',
                              section, drop_options)
                    for o in drop_options:
                        cfg.remove_option(section, o)

                    changed = True

            for option in sorted_options(section, s):
                value = s[option]
                if value is None or (not value and is_lines(section, option)):
                    continue

                old = cfg.get(section, option, fallback=None)
                if old == value:
                    continue

                if not replace:
                    if old is None:
                        log.debug('cfg add option (%r, %r)', section, option)
                    log.debug('cfg.set_option(%r, %r, %r)', section, option, value)
                cfg.set(section, option, value)

                changed = True

        if changed:
            if not replace:
                log.info('write cfg.to_file(%r)', path)
            cfg.to_file(path)

            files_written += 1
            if not (files_written % progress_after):
                log.info(f'%s {basename} files written', f'{files_written:_d}')

    log.info(f'%s {basename} files written total', f'{files_written:_d}')
    return files_written
