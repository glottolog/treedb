import treedb

GLOTTOLOG_TAG = 'v4.1'

CHECKSUM = 'ba2569945c4542f388554b51b98e4fc8d063cb76602be4994b627af7c4400e72'


def test_checksum():
    treedb.configure(engine=None)

    treedb.checkout_or_clone(GLOTTOLOG_TAG)

    treedb.load(exclude_raw=True)

    assert treedb.checksum() == ('path_json:id:sha256:' + CHECKSUM)
