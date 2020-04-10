import treedb


def test_checksum():
    treedb.configure(engine=None)

    if not treedb.root.exists():
        raise NotImplementedError

    treedb.load(exclude_raw=True)

    assert treedb.checksum() == ('path_json:id:sha256:'
                                 'ba2569945c4542f388554b51b98e4fc8'
                                 'd063cb76602be4994b627af7c4400e72')
