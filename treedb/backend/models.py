"""Dataset, producer, and config metadata."""

import logging
import warnings

import sqlalchemy as sa

from .._globals import REGISTRY as registry  # noqa: N811

from .. import _tools
from .. import backend as _backend

__all__ = ['Dataset', 'Producer', 'Config']


log = logging.getLogger(__name__)


@registry.mapped
class Dataset:
    """Git commit loaded into the database."""

    __tablename__ = '__dataset__'

    id = sa.Column(sa.Integer, sa.CheckConstraint('id = 1'), primary_key=True)

    title = sa.Column(sa.Text, sa.CheckConstraint("title != ''"), nullable=False)

    git_commit = sa.Column(sa.String(40), sa.CheckConstraint('length(git_commit) = 40'),
                           nullable=False, unique=True)
    git_describe = sa.Column(sa.Text, sa.CheckConstraint("git_describe != ''"),
                             nullable=False, unique=True)
    clean = sa.Column(sa.Boolean(create_constraint=True), nullable=False)

    version = sa.Column(sa.Text, sa.CheckConstraint("version != ''"))

    exclude_raw = sa.Column(sa.Boolean(create_constraint=True), nullable=False)

    @classmethod
    def get_dataset(cls, /, *, bind, strict, fallback=None):
        table = cls.__tablename__
        log.debug('read %r from %r', table, bind)

        try:
            result, = _backend.iterrows(sa.select(cls), mappings=True, bind=bind)
        except sa.exc.OperationalError as e:
            if 'no such table' in e.orig.args[0]:
                pass
            else:
                log.exception('error selecting %r', table)
                if strict:  # pragma: no cover
                    raise RuntimeError('failed to select %r from %r', table, bind) from e
            return fallback
        except ValueError as e:
            log.exception('error selecting %r', table)
            if 'not enough values to unpack' in e.args[0] and not strict:
                return fallback
            else:  # pragma: no cover
                raise RuntimeError('failed to select %r from %r', table, bind) from e
        except Exception as e:  # pragma: no cover
            log.exception('error selecting %r', table)
            raise RuntimeError('failed to select %r from %r', table, bind) from e
        else:
            return result

    @classmethod
    def log_dataset(cls, params, /, *,
                    ignore_dirty: bool = False,
                    also_print: bool = False, print_file=None):
        name = cls.__tablename__
        log.info('git describe %(git_describe)r clean: %(clean)r', params)
        log.debug('%s.title: %r', name, params['title'])
        log.info('%s.git_commit: %r', name, params['git_commit'])
        if 'version' in params:
            log.info('%s.version: %r', name, params['version'])
        log.debug('%s.exclude_raw: %r', name, params['exclude_raw'])
        if also_print or print_file is not None:
            print('git describe {git_describe!r}'
                  ' clean: {clean!r}'.format_map(params),
                  file=print_file)
            print(f"{name}.title: {params['title']!r}'",
                  file=print_file)
            print(f"{name}.git_commit: {params['git_commit']!r}",
                  file=print_file)
            if 'version' in params:
                print(f"{name}.version: {params['version']!r}",
                      file=print_file)
            print(f"{name}.exclude_raw: {params['exclude_raw']!r}",
                  file=print_file)

        if not params['clean'] and not ignore_dirty:
            warnings.warn(f'{name} not clean,'
                          ' pass ignore_dirty=True to disable')  # pragma: no cover


@registry.mapped
class Producer:
    """Name and version of the package that created a __dataset__."""

    __tablename__ = '__producer__'

    id = sa.Column(sa.Integer, sa.CheckConstraint('id = 1'), primary_key=True)

    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"),
                     unique=True, nullable=False)

    version = sa.Column(sa.Text, sa.CheckConstraint("version != ''"),
                        nullable=False)

    @classmethod
    def get_producer(cls, /, *, bind):
        result, = _backend.iterrows(sa.select(cls), mappings=True, bind=bind)
        return result

    @classmethod
    def log_producer(cls, params, /, *,
                     also_print: bool = False, print_file=None):
        name = cls.__tablename__
        log.info('%s.name: %s', name, params['name'])
        log.info('%s.version: %s', name, params['version'])
        if also_print or print_file is not None:
            print(f"{name}.name: {params['name']}", file=print_file)
            print(f"{name}.version: {params['version']}", file=print_file)


@registry.mapped
class Config:
    """Configuration setting from ``glottolog/config/*.ini``."""

    __tablename__ = '_config'

    filename = sa.Column(sa.String, sa.CheckConstraint("filename != ''"),
                         primary_key=True)
    section = sa.Column(sa.String, sa.CheckConstraint("section != ''"),
                        primary_key=True)
    option = sa.Column(sa.String, sa.CheckConstraint("option != ''"),
                       primary_key=True)

    value = sa.Column(sa.Text, sa.CheckConstraint("value != ''"),
                      nullable=False)

    line = sa.Column(sa.Integer, sa.CheckConstraint('line > 0'),
                     nullable=False)

    __table_args__ = (sa.UniqueConstraint(filename, line),
                      {'info': {'without_rowid': True}})

    @classmethod
    def load(cls, filename: str, /, *, bind,
             _groupby_section=_tools.groupby_itemgetter(0)):
        select_values = (sa.select(Config.section, Config.option, Config.value)
                        .filter_by(filename=filename)
                        .order_by('section', 'option'))
        result = _backend.iterrows(select_values, bind=bind)
        return {section: {option: value for _, option, value in grp}
                for section, grp in _groupby_section(result)}
