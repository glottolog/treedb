import pytest

@pytest.fixture(scope='session')
def treedb_raw(treedb):
    import treedb.raw

    return treedb.raw


def test_print_stats(capsys, treedb_raw):
    assert treedb_raw.print_stats() is None

    out, err = capsys.readouterr()
    assert not err
    assert out.strip()


@pytest.mark.parametrize('tag, weak, expected', [
    ('v4.1', False, ('strong:sha256:'
                     '1d5a043b6cff9b2adb073e94eb67f5d4'
                     '789b3b8f215c1eb7a3f26d0d1858d90a')),
    ('v4.1', True, ('weak:sha256:'
                   '2380ef917237713ac2d6710c05bb6264'
                   '8f9dafa40024550906674a5135d05e3b')),
    ('v4.1', 'unordered', ('unordered:sha256:'
                           'dc6ed1762d47dec12432b09e0d1a1159'
                           '153f062893bd884e8f21ec6b9e42d6c8')),
])
def test_checksum(treedb_raw, tag, weak, expected):
    if pytest.treedb.exclude_raw:
        pytest.skip('test skipped by --exclude-raw')

    if tag != pytest.treedb.glottolog_tag:
        expected = None

    result = treedb_raw.checksum(weak=weak)

    if expected is None:
        assert result
    else:
        assert result == expected
