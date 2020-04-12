# test_languoids.py

import itertools


def test_iterlanguoids(bare_treedb, n=100):
    pairs = bare_treedb.iterlanguoids()
    first = list(itertools.islice(pairs, n))

    assert first
    assert len(first) == n
    for p, l in first:
        assert isinstance(p, tuple)
        assert p
        assert isinstance(l, dict)
        assert l
        assert l['name']


def test_iterrecords(bare_treedb, n=100):
    languoids = bare_treedb.iterlanguoids()
    pairs = bare_treedb.languoids.iterrecords(languoids)
    first = list(itertools.islice(pairs, n))

    assert first
    assert len(first) == n
    for p, r in first:
        assert isinstance(p, tuple)
        assert p
        assert isinstance(r, dict)
        assert r
        assert r['core']['name']
