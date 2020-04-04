# logging.py - logging configuration

import logging
import logging.config

__all__ = ['configure_logging',
           'configure_logging_from_file']

WARNINGS = 'py.warnings'

SQL = 'sqlalchemy.engine'


log = logging.getLogger(__name__)


def configure_logging_from_file(path, *,
                                level=None,
                                log_sql=None,
                                capture_warnings=True,
                                reset=True):
    if reset:
        _reset_logging()

    log.debug('logging.config.fileConfig(%r)', path)
    logging.config.fileConfig(path, disable_existing_loggers=reset)

    logger = logging.getLogger(__package__)
    
    if level is not None:
        for h in logger.handlers:
            if not isinstance(h, logging.FileHandler):
                log.debug('%r.setLevel(%r)', h, level)
                h.setLevel(level)

    if log_sql is not None:
        logging.getLogger(SQL).setLevel('INFO' if log_sql else 'WARNING')

    _set_capture_warnings(capture_warnings)

    return logger


def _reset_logging():
    log.debug('reset logging config')
    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
        h.close()
    

def _set_capture_warnings(value=True):
    log.debug('logging.captureWarnings(%r)', value)
    logging.captureWarnings(value)
    
    
def configure_logging(*,
                      level='WARNING',
                      log_sql=False,
                      format='[%(levelname)s@%(name)s] %(message)s',
                      capture_warnings=True,
                      reset=True):

    if reset:
        _reset_logging()

    cfg = {'version': 1,
           'root': {'handlers': ['stderr_pretty'], 'level': level},
           'loggers': {WARNINGS: {},
                       SQL: {'handlers': ['stderr_plain'],
                             'level': 'INFO' if log_sql else 'WARNING',
                             'propagate': False},
                       __package__: {}},
           'handlers': {'stderr_plain': {'formatter': 'plain',
                                         'class': 'logging.StreamHandler'},
                        'stderr_pretty': {'formatter': 'pretty',
                                          'class': 'logging.StreamHandler'}},
           'formatters': {'plain': {'format': '%(message)s'},
                          'pretty': {'format': format}}}

    log.debug('logging.config.dictConfig(%r)', cfg)
    logging.config.dictConfig(cfg)

    _set_capture_warnings(capture_warnings)

    return logging.getLogger(__package__)
