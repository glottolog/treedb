#  config.py - load configuration from treedb.ini

import configparser
import logging
import os

from . import tools as _tools

__all__ = ['get_default_root',
           'configure']

PATH = 'treedb.ini'

ROOT = 'glottolog', 'repo_root'

ENGINE = 'treedb', 'engine'


log = logging.getLogger(__name__)


def get_default_root(env_var, *, config_path=PATH, fallback):
    """Return default root from environment variable, config, or fallback."""
    root = os.getenv(env_var)

    if root is None:
        cfg = _load_config_file(config_path, default_repo_root=fallback)
        root = cfg.get(*ROOT)

    return root


def _load_config_file(path, *, default_repo_root):
    log.info('load optional config file')
    cfg = configparser.ConfigParser()
    cfg.add_section(ROOT[0])
    cfg.set(*ROOT, default_repo_root)

    found = cfg.read([path])
    log.debug('cfg.read() config file(s): %r', found)
    return cfg


def configure(config_path=PATH, default_repo_root='.'):
    log.info('configure from %r', config_path)
    log.debug('deault repo root: %r', default_repo_root)

    from . import (files,
                   backend,
                   shortcuts)

    config_path = _tools.path_from_filename(config_path)
    if not config_path.exists():
        raise ValueError(r'config file not found: {config_path!r}')

    cfg = _load_config_file(config_path, default_repo_root=default_repo_root)

    log.info('configure logging from %r', config_path)
    shortcuts.configure_logging_from_file(config_path)

    root = cfg.get(*ROOT)
    files.set_root(root)

    engine = cfg.get(*ENGINE, fallback=None)
    backend.set_engine(engine)
