# logging_.py - stdlib logging configuration short-cuts

import logging
import logging.config

__all__ = ['configure_logging',
           'configure_logging_from_file']

WARNINGS = 'py.warnings'

SQL = 'sqlalchemy.engine'

FORMAT = '[%(levelname)s@%(name)s] %(message)s'


log = logging.getLogger(__name__)


def configure_logging_from_file(path, *,
                                level=None,
                                log_sql=None,
                                capture_warnings=True,
                                reset=True):
    if reset:
        reset_logging()

    log.debug('logging.config.fileConfig(%r)', path)
    logging.config.fileConfig(path, disable_existing_loggers=reset)

    package_logger = logging.getLogger(__package__)

    if level is not None:
        if not package_logger.handlers:
            log.debug('set level of %r to %r', package_logger, level)
            package_logger.setLevel(level)
        else:
            for h in package_logger.handlers:
                if not isinstance(h, logging.FileHandler):
                    log.debug('set level of %r to %r', h, level)
                    h.setLevel(level)

    if log_sql is not None:
        sql_logger = logging.getLogger(SQL)
        log.debug('set level of %r to %r', sql_logger, level)
        sql_logger.setLevel('INFO' if log_sql else 'WARNING')

    log_version()

    set_capture_warnings(capture_warnings)

    return package_logger


def reset_logging():
    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        log.debug('%r.removeHandler(%r)', root_logger, h)
        root_logger.removeHandler(h)
        h.close()


def log_version():
    import treedb

    log.info('%s version: %s', __package__, treedb.__version__)
    log.debug('%r', treedb)


def set_capture_warnings(value=True):
    log.debug('set logging.captureWarnings(%r)', value)
    logging.captureWarnings(value)


def configure_logging(*,
                      level='WARNING',
                      log_sql=False,
                      format=FORMAT,
                      capture_warnings=True,
                      reset=True):
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

    if reset:
        reset_logging()

    log.debug('logging.config.dictConfig(%r)', cfg)
    logging.config.dictConfig(cfg)

    log_version()

    set_capture_warnings(capture_warnings)

    return logging.getLogger(__package__)
