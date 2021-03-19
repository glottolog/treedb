# test_records.py

import itertools


def test_records_from_languoids(bare_treedb, n=100):
    languoids = bare_treedb.iterlanguoids()
    pairs = bare_treedb.records.records_from_languoids(languoids)

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
