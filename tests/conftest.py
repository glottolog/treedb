# conftest.py - pytest command-line flags and fixtures

import types

import pytest

GLOTTOLOG_TAG = 'v4.1'


def pytest_addoption(parser):
    parser.addoption('--file-engine', action='store_true',
                     help='use configured file engine instead of in-memory db')

    parser.addoption('--glottolog-tag', default=GLOTTOLOG_TAG,
                     help='tag or branch to clone from Glottolog master repo')

    parser.addoption('--rebuild', action='store_true',
                     help='pass rebuild=True to treedb.load()')

    parser.addoption('--exclude-raw', dest='exclude_raw', action='store_true',
                     help='pass exlcude_raw=True to treedb.load()')

    parser.addoption('--loglevel-debug', action='store_true',
                     help='pass loglevel=DEBUG to treedb.configure()')


def pytest_configure(config):
    options = ('file_engine', 'glottolog_tag',
               'rebuild', 'exclude_raw',
               'loglevel_debug')

    options = {o: config.getoption(o) for o in options}

    pytest.FLAGS = types.SimpleNamespace(**options)


@pytest.fixture(scope='session')
def bare_treedb():
    import treedb as bare_treedb

    kwargs = {} if pytest.FLAGS.file_engine else {'engine': None}

    if pytest.FLAGS.loglevel_debug:
        kwargs['loglevel'] = 'DEBUG'

    bare_treedb.configure(**kwargs)

    bare_treedb.checkout_or_clone(pytest.FLAGS.glottolog_tag)

    return bare_treedb


@pytest.fixture(scope='session')
def treedb(bare_treedb):
    bare_treedb.load(rebuild=pytest.FLAGS.rebuild,
                     exclude_raw=pytest.FLAGS.exclude_raw)
    treedb = bare_treedb
    return treedb
