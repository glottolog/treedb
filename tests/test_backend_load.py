

def test_load(treedb):
    p, l = next(treedb.iterlanguoids(treedb.ENGINE))

    assert isinstance(p, tuple)
    assert isinstance(p, tuple)
    assert p
    assert isinstance(l, dict)
    assert l
    assert l['name']
