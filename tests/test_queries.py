# test_queries.py

import pytest

QUERY_HASH = {'v4.1': ('51569805689a929ad9eec83c0345566f'
                       'b2ae26e8e0c28fc3a046a4a2dc1ee29d')}

STATS = {'v4.1': '''\
24,701 languoids
   241 families
   188 isolates
   429 roots
 8,506 languages
 4,170 subfamilies
11,784 dialects
 7,596 Spoken L1 Languages
   194 Sign Language
   122 Unclassifiable
    80 Pidgin
    67 Unattested
    28 Artificial Language
    14 Mixed Language
    10 Speech Register
 8,111 All
   395 Bookkeeping
'''}

MB = 2**20


def test_print_rows(capsys, treedb):
    query = treedb.select([treedb.Languoid])\
            .where(treedb.Languoid.iso639_3 == 'bsa')

    format_ = '{id}: {name} ({level}) [{iso639_3}]'

    assert treedb.print_rows(query, format_=format_, verbose=True) is None

    out, err = capsys.readouterr()
    assert not err
    assert out == '''\
SELECT languoid.id, languoid.name, languoid.level, languoid.parent_id, languoid.hid, languoid.iso639_3, languoid.latitude, languoid.longitude 
FROM languoid 
WHERE languoid.iso639_3 = ?
abin1243: Abinomn (language) [bsa]
'''


def test_write_csv(treedb):
    expected = QUERY_HASH.get(pytest.treedb.glottolog_tag)
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_csv()

    assert path.name == f'treedb{suffix}.query.csv'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 30 * MB
    if expected is not None:
        assert treedb.tools.sha256sum(path) == expected


def test_hash_csv(treedb):
    expected = QUERY_HASH.get(pytest.treedb.glottolog_tag)

    result = treedb.hash_csv()

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected


@pytest.mark.parametrize('raw', [False, True])
def test_write_json_query_csv(treedb, raw):
    suffix = '-memory' if treedb.ENGINE.file is None else ''
    raw_suffix = '_raw' if raw else ''

    path = treedb.write_json_query_csv(raw=raw)

    assert path.name == f'treedb{suffix}.languoids-json_query{raw_suffix}.csv'
    assert path.exists()
    assert path.is_file()
    assert 5 * MB <= path.stat().st_size <= 100 * MB


def test_print_languoid_stats(capsys, treedb):
    expected = STATS.get(pytest.treedb.glottolog_tag)

    assert treedb.print_languoid_stats() is None

    out, err = capsys.readouterr()
    assert not err
    if expected is None:
        assert out
    else:
        assert out == expected


def test_iterdescendants(treedb):
    pairs = treedb.iterdescendants(parent_level='top', child_level='language')

    first = next(pairs)

    assert first == ('abkh1242', ['abaz1241', 'abkh1244',
                                  'adyg1241', 'kaba1278',
                                  'ubyk1235'])
