CHECKSUM = 'ba2569945c4542f388554b51b98e4fc8d063cb76602be4994b627af7c4400e72'


def test_checksum(treedb, expected=f'path_json:id:sha256:{CHECKSUM}'):
    assert treedb.checksum() == expected
