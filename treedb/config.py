"""Load ``glottolog/config/*.ini`` files."""

import logging
import typing

from . import _globals
from . import _tools

from . import languoids as _languoids

__all__ = ['iterconfigs',
           'load_config']

CONFIG_IN_ROOT = _tools.path_from_filename('config')


log = logging.getLogger(__name__)


def get_config_path(root=_globals.ROOT,
                    *, configpath=CONFIG_IN_ROOT):
    return _languoids.get_repo_root(root) / configpath


def iterconfigs(root=_globals.ROOT, *, glob='*.ini'):
    for path in get_config_path().glob(glob):
        yield path.name, load_config(path)


def load_config(filepath, *, sort_sections: bool = False
                ) -> typing.Dict[str, typing.Dict[str, str]]:
    log.debug('open config file from path: %r', filepath)
    cfg = _tools.ConfigParser.from_file(filepath)

    log.debug('parsed %d section: %r', len(cfg), cfg)
    return cfg.to_dict(sort_sections=sort_sections)
