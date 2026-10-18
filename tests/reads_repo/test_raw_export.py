import pytest

from helpers import assert_file_size_between

CHECKSUM = {('v5.3', False): 'strong:sha256:df14b30a4d046a69fbba594a155f6a35a97d2fa765e5071ad83b6efa89794175',  # noqa: E501
            ('v5.3', True): 'weak:sha256:eb255038e1d8e577727098fd3c8b5cb9f534af0d9fc5cb38dffcc21b017d0943',
            ('v5.2.1', False): 'strong:sha256:e0f1c60ee4ef83c92a4ebc430cf504ac7cc9cb75bb1b10b12b2e2dbc1e20e281',  # noqa: E501
            ('v5.2.1', True): 'weak:sha256:4919879262a20b088a3e1753b102febcbdd1edab14e14bddf325b184fa321681',
            ('v5.2', False): 'strong:sha256:8cd3a20630aaab34aa48bc7e86dec1c69c6b0d139afe1d64349048129447a7ec',
            ('v5.2', True): 'weak:sha256:91f98cd56e5bd9f04e4f42cfbc2d2b6f673627bafb9098e5c2a15faea0ea40bb',
            ('v5.1', False): 'strong:sha256:67e84364cca2eb6ce221777c3a3b965862b0231e841e20a3c8b65a50b3c68e31',
            ('v5.1', True): 'weak:sha256:8637e556a46c3a75fe726bb0d2ed4791770b5939f0ba1146a9457d4ad5b19b37',
            ('v5.0', False): 'strong:sha256:7481bfe65789151b36764f9109fefd7f2134052d49fd3d38135b994f772f8db0',
            ('v5.0', True): 'weak:sha256:e7ed35a867352e39fa4bad935b6732dc3aa4791e6cfd5e6f8fc9c20322e200b5'}

RAW_CSV_SHA256 = {'master': None,
                  'v5.3': 'fbda8b27ee3cf3a8039961af7550224b2b183fba448f4ed4f53212cccfed15ed',
                  'v5.2.1': '00e6591715893684a5c1acdf284654308f1fb1de9c86b19528a41b1426eb2076',
                  'v5.2': '8a226215fd5db1d6fad6b025280cdfd4b61ad87b08d02c8476ead352e3814de6',
                  'v5.1': '640acf6fccf0640fc67b22ab36e0b02f761ece8a90740b6c6c01db2e05b461f0',
                  'v5.0': 'a324b2367331ca7a89d3d6fb4b4fe63fa89ff5caaff4bacfefebc491ee8100e1'}


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
