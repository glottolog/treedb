import pytest

GLOTTOLOG_TAG = 'v4.1'


@pytest.fixture(scope='session')
def bare_treedb(*, tag_or_branch=GLOTTOLOG_TAG, file_engine=False):
    import treedb

    kwargs = {} if file_engine else {'engine': None}
    treedb.configure(**kwargs)

    treedb.checkout_or_clone(GLOTTOLOG_TAG)
    return treedb


@pytest.fixture(scope='session')
def treedb(bare_treedb, exclude_raw=True):
    bare_treedb.load(exclude_raw=exclude_raw)
    return bare_treedb
