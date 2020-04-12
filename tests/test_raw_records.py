import pytest

import itertools


def test_iterlanguoids_from_raw(treedb, n=501):
    if pytest.treedb.exclude_raw:
        pytest.skip('test skipped by --exclude-raw')

    pairs = treedb.iterlanguoids(treedb.ENGINE, from_raw=True)
    first = list(itertools.islice(pairs, n))
    assert len(first) == n
    for p, l in first:
        assert isinstance(p, tuple) and p
        assert isinstance(l, dict) and l
        assert l['name']
