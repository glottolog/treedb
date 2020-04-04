#  config.py - load configuration from treedb.ini

import configparser
import logging
import os

from . import tools as _tools

from . import CONFIG, DEFAULT_ROOT

__all__ = ['get_default_root',
           'configure']

ROOT = 'glottolog', 'repo_root'

ENGINE = 'treedb', 'engine'


log = logging.getLogger(__name__)


def get_default_root(*, env_var, config_path, fallback):
    """Return default root from environment variable, config, or fallback."""
    root = os.getenv(env_var)

    if root is None:
        log.debug('get %r from optional config file %r', ROOT, config_path)
        cfg = _load_config_file(config_path, default_repo_root=fallback)
        root = cfg.get(*ROOT)

    return root


def _load_config_file(path, *, default_repo_root):
    cfg = configparser.ConfigParser()
    cfg.add_section(ROOT[0])
    cfg.set(*ROOT, default_repo_root)

    found = cfg.read([path])
    if found:
        log.debug('cfg.read() config file(s): %r', found)
    else:
        log.debug('no config file(s) found')
    return cfg


def configure(config_path=CONFIG, *, loglevel=None, log_sql=None,
              default_repo_root=DEFAULT_ROOT):
    log.info('configure from %r', config_path)
    log.debug('default repo root: %r', default_repo_root)

    from . import (logging as _logging,
                   files,
                   backend)

    config_path = _tools.path_from_filename(config_path)
    if not config_path.exists():
        raise ValueError(f'config file not found: {config_path!r}')

    log.debug('load config file %r', config_path)
    cfg = _load_config_file(config_path,
                            default_repo_root=default_repo_root)

    log.info('configure logging from %r', config_path)
    _logging.configure_logging_from_file(cfg, level=loglevel, log_sql=log_sql)

    root = cfg.get(*ROOT)
    root = _tools.path_from_filename(root)
    if not root.is_absolute():
        root = config_path.parent / root
    files.set_root(root)

    engine = cfg.get(*ENGINE, fallback=None)
    if engine is not None:
        engine = _tools.path_from_filename(engine)
        if not engine.is_absolute():
            engine = config_path.parent / engine
    backend.set_engine(engine)
