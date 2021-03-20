import itertools

import pytest


pytestmark = pytest.FLAGS.skip_exclude_raw


def test_iterlanguoids_from_raw(treedb, n=501):
    pairs = treedb.iterlanguoids(treedb.engine, from_raw=True)

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
