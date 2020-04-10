import pytest

@pytest.fixture(scope='session')
def treedb_raw():
    import treedb.raw

    return treedb.raw


@pytest.mark.parametrize('weak, expected', [
    (False, ('strong:sha256:'
             '1d5a043b6cff9b2adb073e94eb67f5d4'
             '789b3b8f215c1eb7a3f26d0d1858d90a')),
    (True, ('weak:sha256:'
            '2380ef917237713ac2d6710c05bb6264'
            '8f9dafa40024550906674a5135d05e3b')),
     ('unordered', ('unordered:sha256:'
                    'dc6ed1762d47dec12432b09e0d1a1159'
                    '153f062893bd884e8f21ec6b9e42d6c8')),
])
def test_checksum(treedb_raw, weak, expected):
    assert treedb_raw.checksum(weak=weak) == expected
