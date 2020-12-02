# test_languoids_json.py

import itertools

import pytest

PREFIX = 'path_json:id:sha256:'

CHECKSUM = {'v4.1': ('d09dd920871bdaaecad609922bd29b90'
                     'bf2f1307a19e144d393e992460092a1d'),
            'v4.2': ('a3c7550c507bab3d7431ef7b772c9f8c'
                     'e27df03b5e4d5ca085d109f869503261'),
            'v4.2.1': ('f3a0127b580e2ac3361af2cf84466777'
                       'd66efad996483b6a92ac29218036e120'),
            'v4.3-treedb-fixes':
                    ('c384ab4887ec10a4f80baa1c36198a94'
                     'f127c46aee768d3680ce3b474d0eac4e')}

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
        shasum = treedb.tools.sha256sum(path)
        assert shasum == expected


def test_checksum(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)

    result = treedb.checksum()

    if expected is None:
        assert result.startswith(PREFIX)
        assert len(result) - len(PREFIX) == 64
    else:
        assert result == PREFIX + expected


@pytest.mark.skipif(pytest.FLAGS.glottolog_tag == 'v4.1',
                    reason='requires https://github.com/glottolog/glottolog/pull/495')
@pytest.mark.parametrize('kwargs', [
    ({'source': 'raw'}, {'source': 'tables'}),
    ({'source': 'files', 'file_order': True},
     {'source': 'raw', 'file_order': True},
     {'source': 'tables', 'file_order': True},
     {'source': 'raw', 'file_order': True, 'file_means_path': False}),
])
def test_checksum_equivalence(treedb, kwargs):
    """Test for equivalence of the serialization from different sources.

    - from the md.ini files ('files')
    - from the file lines loaded into raw tables  ('raw')
    - from the parsed table contents ('tables')

    This should pass for glottolog release candidates.

    This is intended to pass at glottolog HEAD with the latest treedb.

    This can fail from manual editing inconsistencies.
    Running treedb.write_files() can be used to write back the table contents
    to the glottolog md.ini files in a soft manner (e.g. not normalizing order
    for cases where it is not significant). If the diff looks correct, this
    can be used to bring the repository files back into a canonicalized
    (i.e. round-tripable) format. A common instance are floats being normalized
    via round-tripping (e.g. dropping trailing zeros).

    Changing the export formatting breaks this intentionally for older versions,
    i.e. for glottolog commits before the change was applied to the
    glottolog repository md.ini files (see xfails below).

    Intended changes usually pertain to normalization/canonicalization,
    ordering, formatting.

    Less frequent changes pertain to changing the data format such as
    removing redundancy.

    -- 
    First shalt thou take out the Holy Pin.

    Then shalt thou count to three, no more, no less.

    Three shall be the number thou shalt count,
    and the number of the counting shall be three.

    Four shalt thou not count, neither count thou two,
    excepting that thou then proceed to three.

    Five is right out.

    Once the number three, being the third number, be reached,
    then lobbest thou thy Holy Hand Grenade of Antioch towards thy foe,
    who, being naughty in My sight, shall snuff it.
    """
    results = [(kw, treedb.checksum(**kw)) for kw in kwargs]

    for kw, r in results:
        if kw.get('file_order', False):
            if kw.get('file_means_path', True):
                ordered = 'path'
            else:
                ordered = 'file'
        else:
            ordered = 'id'
        prefix = f'path_json:{ordered}:sha256:'
        assert r.startswith(prefix)
        assert len(r) - len(prefix) == 64

    if pytest.FLAGS.glottolog_tag in ('v4.1',
                                      'v4.2', 'v4.2.1',
                                      'v4.3-treedb-fixes'):
        pytest.xfail('format change: minimal countries')

    for (c, cur), (n, nxt) in pairwise(results):
        assert cur[-64:] == nxt[-64:], f'checksum(**{c!r}) == checksum(**{n!r})'
