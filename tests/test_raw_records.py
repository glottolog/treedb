# test_raw_records.py

import pytest

import itertools


pytestmark = pytest.mark.skipif(pytest.FLAGS.exclude_raw,
                                reason='skipped by --exclude-raw')


def test_iterlanguoids_from_raw(treedb, n=501):
    pairs = treedb.iterlanguoids(treedb.ENGINE, from_raw=True)
    first = list(itertools.islice(pairs, n))

    assert first
    assert len(first) == n

    for p, l in first:
        assert isinstance(p, tuple)
        assert p

        assert isinstance(l, dict)
        assert l
        assert l['name']
