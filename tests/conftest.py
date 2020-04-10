import pytest

GLOTTOLOG_TAG = 'v4.1'


def pytest_addoption(parser):
    parser.addoption('--file-engine', action='store_true',
                     help='use configured file engine instead of in-memory db')

    parser.addoption('--exclude-raw', dest='exclude_raw', action='store_true',
                     help='pass exlcude_raw=True to treedb.load()')

    parser.addoption('--glottolog-tag', default=GLOTTOLOG_TAG,
                     help='tag or branch in Glottolog master repo')


@pytest.fixture(scope='session')
def bare_treedb(request):
    import treedb as bare_treedb

    file_engine = request.config.getoption('file_engine')
    kwargs = {} if file_engine else {'engine': None}
    bare_treedb.configure(**kwargs)

    glottolog_tag = request.config.getoption('glottolog_tag')
    bare_treedb.checkout_or_clone(glottolog_tag)

    return bare_treedb


@pytest.fixture(scope='session')
def treedb(request, bare_treedb):
    exclude_raw = request.config.getoption('exclude_raw')

    bare_treedb.load(exclude_raw=exclude_raw)

    treedb = bare_treedb
    return treedb
