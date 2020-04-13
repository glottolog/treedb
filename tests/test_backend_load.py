# test_backend_load.py

import itertools


def test_load(treedb, n=10):
    pairs = treedb.iterlanguoids(treedb.ENGINE)
    first = list(itertools.islice(pairs, n))

    assert first
    assert len(first) == n

    for p, l in first:
        assert isinstance(p, tuple)
        assert p

        assert isinstance(l, dict)
        assert l
        assert l['name']
