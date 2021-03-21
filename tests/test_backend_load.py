from conftest import assert_valid_languoids


def test_load(treedb, n=100):
    items = treedb.iterlanguoids(treedb.engine)
    assert_valid_languoids(items, n=n)
