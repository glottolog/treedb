"""Load and write ``glottolog/languoids/tree/**/md.ini``."""

import configparser
import functools
import logging
import os
import typing
import warnings

from .. import _globals
from .. import _tools

from . import fields as _fields

__all__ = ['set_root', 'get_repo_root',
           'iterfiles',
           'roundtrip', 'write_files']

TREE_IN_ROOT = _tools.path_from_filename('languoids', 'tree')

BASENAME = 'md.ini'


log = logging.getLogger(__name__)


def set_root(repo_root, *, resolve=False,
             treepath=TREE_IN_ROOT):
    """Set and return default root for glottolog lanugoid directory tree."""
    log.info('set_root: %r', repo_root)
    if repo_root is None:
        raise ValueError(f'missing repo_root path: {repo_root!r}')

    repo_path = _tools.path_from_filename(repo_root)
    if resolve:
        repo_path = repo_path.resolve(strict=False)

    ROOT = _globals.ROOT
    ROOT.path = repo_path / _tools.path_from_filename(treepath)
    return ROOT


def get_repo_root(root=_globals.ROOT,
                  *, treepath=TREE_IN_ROOT):
    assert root.parts[-len(treepath.parts):] == treepath.parts
    repo_root = _tools.path_from_filename(root)
    for _ in treepath.parts:
        repo_root = repo_root.parent
    return repo_root


class ConfigParser(_tools.ConfigParser):
    """ConfigParser for ``glottolog/languoids/tree/**/md.ini``."""

    _basename = BASENAME

    _newline = '\r\n'

    _header = '# -*- coding: {encoding} -*-\n'

    def update_config(self, raw_record: _fields.RawRecordType,
                      *, replace: bool = False,
                      quiet: bool = False,
                      is_lines=_fields.is_lines,
                      core_sections=_fields.CORE_SECTIONS,
                      omit_empty_core_options=_fields.OMIT_EMPTY_CORE_OPTIONS,
                      keep_empty_sections=_fields.KEEP_EMPTY_SECTIONS,
                      keep_empty_options=_fields.KEEP_EMPTY_OPTIONS,
                      sorted_sections=_fields.sorted_sections,
                      sorted_options=_fields.sorted_options) -> bool:
        if replace:
            self.clear()

        for core_section in core_sections:
            s = raw_record[core_section]
            for core_option in omit_empty_core_options:
                if core_option in s and not s[core_option]:
                    del s[core_option]

        changed = False

        old_sections = set(self.sections())
        old_empty = {s for s in old_sections if not any(v for _, v in self.items(s))}

        new_sections = {sec for sec, s in raw_record.items() if s}
        leave = (old_empty - new_sections) & keep_empty_sections

        drop = old_sections - core_sections - leave - new_sections
        if drop:
            drop = sorted_sections(drop)
            log.debug('cfg.remove_section(s) for s in %r', drop)
            for s in drop:
                self.remove_section(s)
            changed = True

        add = new_sections - old_sections
        if add:
            add = sorted_sections(add)
            if not quiet:
                log.debug('cfg.add_section(s) for s in %r', add)
            for s in add:
                self.add_section(s)
            changed = True

        for section in sorted_sections(raw_record):
            if section in drop:
                continue

            s = raw_record[section]

            if section not in old_sections or section in core_sections:
                pass
            elif section in leave:
                continue
            else:
                old_options = {o for o in self.options(section)
                               if (section, o) not in keep_empty_options}
                new_options = {k for k, v in s.items() if v}
                drop_options = old_options - new_options

                if drop_options:
                    drop_options = sorted_options(section, drop_options)
                    log.debug('cfg.remove_option(%r, o) for o in %r',
                              section, drop_options)
                    for o in drop_options:
                        self.remove_option(section, o)
                    changed = True

            for option in sorted_options(section, s):
                value = s[option]
                if value is None or (not value and is_lines(section, option)):
                    continue

                old = self.get(section, option, fallback=None)
                if old == value:
                    continue

                if not quiet:  # pragma: no cover
                    if old is None:
                        log.debug('cfg add option (%r, %r)', section, option)
                    log.debug('cfg.set_option(%r, %r, %r)', section, option, value)

                self.set(section, option, value)
                changed = True

        return changed


class FileInfo(typing.NamedTuple):
    """Triple of ((<path_part>, ...), <DirEntry object>, <ConfigParser object>)."""

    path: _globals.PathType

    dentry: os.DirEntry

    config: ConfigParser

    @classmethod
    def from_dentry(cls, dentry: os.DirEntry,
                    *, path_slice: slice = slice(None)):
        path = _tools.path_from_filename(dentry)
        config = ConfigParser.from_file(path)
        return cls(path.parts[path_slice], dentry, config)


def iterfiles(root=_globals.ROOT,
              *, progress_after: int = _tools.PROGRESS_AFTER
              ) -> typing.Iterator[FileInfo]:
    """Yield triples of ((<path_part>, ...), <ConfigParser object>, <DirEntry object>)."""
    root = _tools.path_from_filename(root).resolve()
    log.info(f'start parsing {BASENAME} files from %r', root)
    msg = f'%s {BASENAME} files parsed'

    kwargs = {'path_slice': slice(len(root.parts), -1)}
    make_fileinfo = functools.partial(FileInfo.from_dentry, **kwargs)

    n = 0
    for n, dentry in enumerate(_tools.walk_scandir(root), start=1):
        yield make_fileinfo(dentry)

        if not (n % progress_after):
            log.info(msg, f'{n:_d}')

    log.info(f'%s {BASENAME} files total', f'{n:_d}')


def roundtrip(root=_globals.ROOT,
              *, progress_after: int = _tools.PROGRESS_AFTER) -> None:
    """Load/save all config files (drops leading/trailing whitespace)."""
    log.info(f'start roundtripping {BASENAME} files in %r', root)
    for path_tuple, dentry, cfg in iterfiles(root, progress_after=progress_after):
        cfg.to_file(dentry.path)


def write_files(records: typing.Iterable[_globals.RecordItem],
                root=_globals.ROOT, *, replace: bool = False,
                dry_run: bool = False, quiet: typing.Optional[bool] = None,
                require_nwritten: typing.Optional[int] = None,
                progress_after: typing.Optional[int] = _tools.PROGRESS_AFTER,
                basename: str = BASENAME) -> int:
    """Write ((<path_part>, ...), <dict of dicts>) pairs to root."""
    if replace:  # pragma: no cover
        if dry_run:
            warnings.warn('replace=True ignored by dry_run=True')
        else:
            log.warning(f'replace present {basename} files')

    if quiet is None:
        quiet = dry_run or replace

    root = _tools.path_from_filename(root)
    log.info(f'start writing {basename} files into %r', root)

    load_config = ConfigParser.from_file

    files_written = 0

    for path_tuple, raw_record in map(_fields.join_lines_inplace, records):
        path = root.joinpath(*path_tuple + (basename,))
        cfg = load_config(path)
        changed = cfg.update_config(raw_record, replace=replace, quiet=quiet)

        if changed:
            if not dry_run:
                if not replace:
                    log.info('write cfg.to_file(%r)', path)
                cfg.to_file(path)

            files_written += 1

            if not dry_run and (files_written % progress_after):
                log.info(f'%s {basename} files written', f'{files_written:_d}')

        if require_nwritten is not None and files_written > require_nwritten:
            raise ValueError(f'files_written={files_written}'
                             f' over require_nwritten={require_nwritten}')

    log.info(f'%s {basename} files written total', f'{files_written:_d}')
    if require_nwritten is not None and files_written < require_nwritten:
        raise ValueError(f'files_written={files_written}'
                         f' under require_nwritten={require_nwritten}')
    return files_written
