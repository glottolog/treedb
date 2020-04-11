import pytest

HASH = {'v4.1': ('51569805689a929ad9eec83c0345566f'
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


def test_hash_csv(treedb, expected=HASH):
    expected = HASH.get(pytest.treedb.glottolog_tag)

    result = treedb.hash_csv()

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected


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
