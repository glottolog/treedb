# test_languoids.py

import pytest

import itertools


def test_iterlanguoids(bare_treedb, n=100):
    pairs = bare_treedb.iterlanguoids()

    head = list(itertools.islice(pairs, n))

    assert head
    assert len(head) == n

    for path, languoid in head:
        assert isinstance(path, tuple)
        assert all(isinstance(p, str) for p in path)
        assert path
        assert all(path)

        assert isinstance(languoid, dict)
        assert languoid
        assert languoid['id']
        assert languoid['parent_id'] is None or languoid['parent_id']
        assert languoid['level'] in ('family', 'language', 'dialect')
        assert languoid['name']


def test_iterrecords(bare_treedb, n=100):
    languoids = bare_treedb.iterlanguoids()
    pairs = bare_treedb.languoids.iterrecords(languoids)

    head = list(itertools.islice(pairs, n))

    assert head
    assert len(head) == n

    for path, record in head:
        assert isinstance(path, tuple)
        assert all(isinstance(p, str) for p in path)
        assert path
        assert all(path)

        assert isinstance(record, dict)
        assert record
        assert record['core']['level'] in ('family', 'language', 'dialect')
        assert record['core']['name']
