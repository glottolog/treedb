# test_languoids_json.py

import pytest

CHECKSUM = {'v4.1': ('ba2569945c4542f388554b51b98e4fc8'
                     'd063cb76602be4994b627af7c4400e72')}

MB = 2**20


def test_write_json_csv(treedb):
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_json_csv()

    assert path.name == f'treedb{suffix}.languoids-json.csv'
    assert path.exists()
    assert path.is_file()
    assert 5 * MB <= path.stat().st_size <= 100 * MB


def test_checksum(treedb, prefix='path_json:id:sha256:'):
    checksum = CHECKSUM.get(pytest.treedb.glottolog_tag)

    result = treedb.checksum()

    if checksum is None:
        assert result.startswith(prefix)
    else:
        assert treedb.checksum() == prefix + checksum
