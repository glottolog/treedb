
import pytest

from helpers import assert_file_size_between

CHECKSUM = {('v5.2.1', False): 'strong:sha256:e0f1c60ee4ef83c92a4ebc430cf504ac7cc9cb75bb1b10b12b2e2dbc1e20e281',  # noqa: E501
            ('v5.2.1', True): 'weak:sha256:4919879262a20b088a3e1753b102febcbdd1edab14e14bddf325b184fa321681',
            ('v5.2', False): 'strong:sha256:8cd3a20630aaab34aa48bc7e86dec1c69c6b0d139afe1d64349048129447a7ec',
            ('v5.2', True): 'weak:sha256:91f98cd56e5bd9f04e4f42cfbc2d2b6f673627bafb9098e5c2a15faea0ea40bb',
            ('v5.1', False): 'strong:sha256:67e84364cca2eb6ce221777c3a3b965862b0231e841e20a3c8b65a50b3c68e31',
            ('v5.1', True): 'weak:sha256:8637e556a46c3a75fe726bb0d2ed4791770b5939f0ba1146a9457d4ad5b19b37',
            ('v5.0', False): 'strong:sha256:7481bfe65789151b36764f9109fefd7f2134052d49fd3d38135b994f772f8db0',
            ('v5.0', True): 'weak:sha256:e7ed35a867352e39fa4bad935b6732dc3aa4791e6cfd5e6f8fc9c20322e200b5',
            ('v4.8', False): 'strong:sha256:f7f31bc8e8e6087bb64d24d7e88fa1c9dc3570ec279174b2c78c0fd91011168e',
            ('v4.8', True): 'weak:sha256:fd231ffb2a44ac6ed979d5e8050aafe6fd49f8e6d53e0499755d1f81a2194f5d',
            ('v4.7', False): 'strong:sha256:a54b735866b496fa5d508d2037d7a6a6452680eb0fc04f4c564f188a8e847def',
            ('v4.7', True): 'weak:sha256:838d735b87dc23da491e89f76fddd7766e9b392963f581788fcc9c5e4ed51161',
            ('v4.6', False): 'strong:sha256:fcc129dbfedc77a8238565e4e12eb53198e70dda9f6613c7f467e5b057dc04ca',
            ('v4.6', True): 'weak:sha256:1610520f11227a5489b44693b14a5d8db423d66d7ace391c2db221a56036457a',
            ('v4.5', False): 'strong:sha256:dda5285990cf8818c217a3dfc03799f2f7a091139a2de6ba08cd0fd18b8f3b94',
            ('v4.5', True): 'weak:sha256:8e56dab63dacb3d1b44e85c55ec3ea04dee8754d8a89e9542fd69df2a0e2f28b',
            ('v4.4', False): 'strong:sha256:3c95e5081c8d689980637506e363f99904798aa1a5ceb69eb1187b8eaff53709',
            ('v4.4', True): 'weak:sha256:690dd35d5de6a0841f3d3f8cc8c9853382797c73e082a418c1b9ad77bd7495bb',
            ('v4.3-treedb-fixes', False): 'strong:sha256:db13f74ed52884084ab38b9693d42a589ff4ebc033ef1ed62b6463f44ea9320b',  # noqa: E501
            ('v4.3-treedb-fixes', True): 'weak:sha256:1f48d9a546fb99cab53eb171806d6a16c5d9affc9c6f49d615692fdbd636c58c',  # noqa: E501
            ('v4.2.1', False): 'strong:sha256:03ae265f46c79a5fd9ae44ada3ed50840dbdb897384b7ac57456ba12b6206a71',  # noqa: E501
            ('v4.2.1', True): 'weak:sha256:9cf661e51d8cd6d8ef1f5e93dbbf4612a8a7e06712c56747d7a280b2d83f503b',
            ('v4.1', False): 'strong:sha256:1d5a043b6cff9b2adb073e94eb67f5d4789b3b8f215c1eb7a3f26d0d1858d90a',
            ('v4.1', True): 'weak:sha256:2380ef917237713ac2d6710c05bb62648f9dafa40024550906674a5135d05e3b',
            ('v4.1', 'unordered'): 'unordered:sha256:dc6ed1762d47dec12432b09e0d1a1159153f062893bd884e8f21ec6b9e42d6c8'}  # noqa: E501

RAW_CSV_SHA256 = {'master': None,
                  'v5.2.1': '00e6591715893684a5c1acdf284654308f1fb1de9c86b19528a41b1426eb2076',
                  'v5.2': '8a226215fd5db1d6fad6b025280cdfd4b61ad87b08d02c8476ead352e3814de6',
                  'v5.1': '640acf6fccf0640fc67b22ab36e0b02f761ece8a90740b6c6c01db2e05b461f0',
                  'v5.0': 'a324b2367331ca7a89d3d6fb4b4fe63fa89ff5caaff4bacfefebc491ee8100e1',
                  'v4.8': '0f1b646c5bc9f3a454e8516d9a0288b086bb8acba673b94c1a4942d3a5d1bdaa',
                  'v4.7': '2b03bfdc8226e0cd0736a227fd74b7e04a511a198b102b70920af3c9f6194529',
                  'v4.6': '5a792c56de62db2695ef29d2344b60196d40d6307acab2a8b72f2b73681ec0b9',
                  'v4.5': '7c8a101b40c41629e6dfd6f5e5bfb627d3b8bc0e85a4444c7f9cff0ad4dbf1c9',
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
