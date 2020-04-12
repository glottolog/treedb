# conftest.py - pytest command-line options and fixtures

import types

import pytest

GLOTTOLOG_TAG = 'v4.1'


def pytest_addoption(parser):
    parser.addoption('--file-engine', action='store_true',
                     help='use configured file engine instead of in-memory db')

    parser.addoption('--glottolog-tag', default=GLOTTOLOG_TAG,
                     help='tag or branch in Glottolog master repo')

    parser.addoption('--rebuild', action='store_true',
                     help='pass rebuild=True to treedb.load()')

    parser.addoption('--exclude-raw', dest='exclude_raw', action='store_true',
                     help='pass exlcude_raw=True to treedb.load()')

    parser.addoption('--loglevel-debug', action='store_true',
                     help='pass loglevel=DEBUG to treedb.configure()')


@pytest.fixture(scope='session')
def bare_treedb(request):
    file_engine = request.config.getoption('file_engine')
    glottolog_tag = request.config.getoption('glottolog_tag')
    loglevel_debug = request.config.getoption('loglevel_debug')

    import treedb as bare_treedb

    kwargs = {} if file_engine else {'engine': None}
    if loglevel_debug:
        kwargs['loglevel'] = 'DEBUG'

    bare_treedb.configure(**kwargs)

    bare_treedb.checkout_or_clone(glottolog_tag)

    pytest.treedb = types.SimpleNamespace(glottolog_tag=glottolog_tag)

    return bare_treedb


@pytest.fixture(scope='session')
def treedb(request, bare_treedb):
    rebuild = request.config.getoption('rebuild')
    exclude_raw = request.config.getoption('exclude_raw')

    bare_treedb.load(rebuild=rebuild, exclude_raw=exclude_raw)
    treedb = bare_treedb

    pytest.treedb.rebuild = rebuild
    pytest.treedb.exclude_raw = exclude_raw

    return treedb
