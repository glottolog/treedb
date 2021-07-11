import pytest

from helpers import assert_file_size_between

CHECKSUM = {('v4.4', False): 'strong:sha256:3c95e5081c8d689980637506e363f99904798aa1a5ceb69eb1187b8eaff53709',
            ('v4.4', True): 'weak:sha256:690dd35d5de6a0841f3d3f8cc8c9853382797c73e082a418c1b9ad77bd7495bb',
            ('v4.3-treedb-fixes', False): 'strong:sha256:db13f74ed52884084ab38b9693d42a589ff4ebc033ef1ed62b6463f44ea9320b',  # noqa: E501
            ('v4.3-treedb-fixes', True): 'weak:sha256:1f48d9a546fb99cab53eb171806d6a16c5d9affc9c6f49d615692fdbd636c58c',  # noqa: E501
            ('v4.2.1', False): 'strong:sha256:03ae265f46c79a5fd9ae44ada3ed50840dbdb897384b7ac57456ba12b6206a71',  # noqa: E501
            ('v4.2.1', True): 'weak:sha256:9cf661e51d8cd6d8ef1f5e93dbbf4612a8a7e06712c56747d7a280b2d83f503b',
            ('v4.1', False): 'strong:sha256:1d5a043b6cff9b2adb073e94eb67f5d4789b3b8f215c1eb7a3f26d0d1858d90a',
            ('v4.1', True): 'weak:sha256:2380ef917237713ac2d6710c05bb62648f9dafa40024550906674a5135d05e3b',
            ('v4.1', 'unordered'): 'unordered:sha256:dc6ed1762d47dec12432b09e0d1a1159153f062893bd884e8f21ec6b9e42d6c8'}  # noqa: E501

RAW_CSV_SHA256 = {'master': None,
                  'v4.4': 'ad0c5a84f36815d25d053649bf2362097283e04b80d336bf4edb1c5ff1795bf6',
                  'v4.3-treedb-fixes': '1ef6923a94d19c708fd0e7ae87b6ee24c69d1d82fa9f81b16eaa5067e61ab1b6',
                  'v4.2.1': 'ab9d4339f3c0fa3acb0faf0f7306dc5409640ecd46e451de9a76445519f5157e',
                  'v4.1': '963163852e7f4ee34b516bc459bdbb908f2f4aab64bda58087a1a23a731921fd'}


pytestmark = pytest.mark.raw


def test_print_stats(capsys, treedb_raw):
    assert treedb_raw.raw.print_stats() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip()


@pytest.mark.parametrize('weak',
                         [False, True, 'unordered'],
                         ids=lambda x: f'weak={x}')
def test_checksum(pytestconfig, treedb_raw, weak):
    expected = CHECKSUM.get((pytestconfig.option.glottolog_tag, weak))

    result = treedb_raw.raw.checksum(weak=weak)

    if expected is None:
        prefix, hash_name, hexdigest = result.split(':')
        assert prefix in ('strong', 'weak', 'unordered')
        assert hash_name == 'sha256'
        assert len(hexdigest) == 64
    else:
        assert result == expected


def test_write_raw_csv(pytestconfig, treedb_raw):
    expected = RAW_CSV_SHA256.get(pytestconfig.option.glottolog_tag)
    suffix = '-memory' if treedb_raw.engine.file is None else ''
    suffix += pytestconfig.option.file_engine_tag

    path = treedb_raw.raw.write_raw_csv()

    assert path.name == f'treedb{suffix}.raw.csv.gz'
    assert_file_size_between(path, 1, 100)

    if expected is None:
        pass
    else:
        shasum = treedb_raw.sha256sum(path)
        assert shasum == expected
