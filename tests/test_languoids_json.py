# test_languoids_json.py

import itertools

import pytest

PREFIX = 'path_json:id:sha256:'

CHECKSUM = {'v4.1': ('c1381233b0ffdb216083dfc3aa84eb27'
                     'ddb3f51578d52f2bfaf1771055f435a8'),
            'v4.2': ('4b34fc6ceda6a3af906ed02499ba5e52a'
                     '39b9f6fa4526d39ab0c8053f3bc305d'),
            'v4.2.1': ('1b85081a5fc1ddedf91f7099524a12c0'
                       'd8941d6ccf3ff9feb889aec46b863e43')}

MB = 2**20


def pairwise(iterable):
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def test_write_json_csv(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_json_csv()

    assert path.name == f'treedb{suffix}.languoids-json.csv.gz'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 100 * MB

    if expected is None:
        pass
    else:
        assert treedb.tools.sha256sum(path) == expected


def test_checksum(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)

    result = treedb.checksum()

    if expected is None:
        assert result.startswith(PREFIX)
        assert len(result) - len(PREFIX) == 64
    else:
        assert treedb.checksum() == PREFIX + expected


@pytest.mark.skipif(pytest.FLAGS.glottolog_tag == 'v4.1',
                    reason='requires https://github.com/glottolog/glottolog/pull/495')
@pytest.mark.parametrize('kwargs', [
    ({'source': 'tables'}, {'source': 'raw'}),
    ({'source': 'tables', 'file_order': True},
     {'source': 'raw', 'file_order': True},
     {'source': 'files', 'file_order': True},
     {'source': 'raw', 'file_order': True, 'file_means_path': False}),
])
def test_checksum_equivalence(treedb, kwargs):
    results = [(kw, treedb.checksum(**kw)) for kw in kwargs]

    for kw, r in results:
        if kw.get('file_order', False):
            if kw.get('file_means_path', True):
                ordered =  'path'
            else:
                ordered = 'file'
        else:
            ordered = 'id'
        prefix = f'path_json:{ordered}:sha256:'
        assert r.startswith(prefix)
        assert len(r) - len(prefix) == 64

    for (c, cur), (n, nxt) in pairwise(results):
        assert cur[-64:] == nxt[-64:], f'checksum(**{c!r}) == checksum(**{n!r})'
