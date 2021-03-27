import itertools

import pytest


@pytest.mark.parametrize('kwargs, expected_head', [
    ({'parent_level': 'top', 'child_level': 'language'},
     [('abin1243', []),
      ('abis1238', []),
      ('abkh1242', ['abaz1241', 'abkh1244',
                                'adyg1241', 'kaba1278',
                                'ubyk1235']),
      ('adai1235', [])]),
])
def test_iterdescendants(treedb, kwargs, expected_head):
    pairs = treedb.iterdescendants(**kwargs)
    head = list(itertools.islice(pairs, len(expected_head)))

    assert head == expected_head
