from conftest import assert_valid_languoids


def test_iterlanguoids(bare_treedb, n=100):
    items = bare_treedb.iterlanguoids()
    assert_valid_languoids(items, n=n)
