# shortcuts.py - sqlalchemy/pandas short-cut functions

import functools
import logging
import logging.config
import warnings

import sqlalchemy as sa

from . import ENGINE

__all__ = ['configure_logging',
           'count', 'select', 'text',
           'pd_read_sql']

ROOT = 'root'

PANDAS = None


log = logging.getLogger(__name__)


def configure_logging(*,
                      level='WARNING',
                      format='[%(levelname)s@%(name)s] %(message)s',
                      capture_warnings=True,
                      log_sql=False,
                      reset=True):

    if reset:
        log.debug('reset logging config')
        root = logging.getLogger(ROOT)
        for h in list(root.handlers):
            root.removeHandler(h)
            h.close()

    cfg = {'version': 1,
           ROOT: {'handlers': ['plain'], 'level': 'INFO'},
           'loggers': {'py.warnings': {'handlers': ['prefixed'],
                                       'level': 'WARNING',
                                       'propagate': False},
                       'sqlalchemy.engine': {'level': ('INFO' if log_sql
                                                       else 'WARNING')},
                       __package__: {'handlers': ['prefixed'],
                                     'propagate': False,
                                     'level': level}},
           'handlers': {'plain': {'formatter': 'plain',
                                  'level': 'DEBUG',
                                  'class': 'logging.StreamHandler'},
                        'prefixed': {'formatter': 'prefixed',
                                    'level': 'DEBUG',
                                    'class': 'logging.StreamHandler'}},
           'formatters': {'plain': {'format': '%(message)s'},
                          'prefixed': {'format': format}}}

    log.debug('logging.config.dictConfig(%r)', cfg)
    logging.config.dictConfig(cfg)
    logging.captureWarnings(True)

    return logging.getLogger(__package__)


count = sa.func.count

select = functools.partial(sa.select, bind=ENGINE)

text = functools.partial(sa.text, bind=ENGINE)


def pd_read_sql(sql=None, *args, con=ENGINE, **kwargs):
    global PANDAS
    if PANDAS is None:
        try:
            import pandas as PANDAS
        except ImportError as e:
            warnings.warn(f'failed to import pandas: {e}')
            return None

    if sql is None:
        from . import queries
        sql = queries.get_query()

    return PANDAS.read_sql_query(sql, *args, con=con, **kwargs)
