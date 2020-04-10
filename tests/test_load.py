

def test_load(treedb):
    assert next(treedb.iterlanguoids(treedb.engine))
