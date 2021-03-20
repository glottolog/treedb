import itertools


def test_load(treedb, n=100):
    pairs = treedb.iterlanguoids(treedb.engine)

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
