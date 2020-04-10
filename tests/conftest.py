import pytest

GLOTTOLOG_TAG = 'v4.1'


@pytest.fixture(scope='session')
def treedb(*, tag_or_branch=GLOTTOLOG_TAG, file_engine=False, exclude_raw=True):
    import treedb

    kwargs = {} if file_engine else {'engine': None}
    treedb.configure(**kwargs)

    treedb.checkout_or_clone(GLOTTOLOG_TAG)
    treedb.load(exclude_raw=exclude_raw)
    return treedb
