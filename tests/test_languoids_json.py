# test_languoids_json.py

import pytest

PREFIX = 'path_json:id:sha256:'

CHECKSUM = {'v4.1': ('ba2569945c4542f388554b51b98e4fc8'
                     'd063cb76602be4994b627af7c4400e72')}

MB = 2**20


def test_write_json_csv(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_json_csv()

    assert path.name == f'treedb{suffix}.languoids-json.csv.gz'
    assert path.exists()
    assert path.is_file()
    assert 5 * MB <= path.stat().st_size <= 100 * MB
    if expected is not None:
        assert treedb.tools.sha256sum(path) == expected


def test_checksum(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)

    result = treedb.checksum()

    if expected is None:
        assert result.startswith(PREFIX)
        assert len(result) - len(PREFIX) == 64
    else:
        assert treedb.checksum() == PREFIX + expected
