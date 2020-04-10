HASH = '51569805689a929ad9eec83c0345566fb2ae26e8e0c28fc3a046a4a2dc1ee29d'


def test_hash_csv(treedb, expected=HASH):
    assert treedb.hash_csv() == expected
