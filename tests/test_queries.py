HASH = '51569805689a929ad9eec83c0345566fb2ae26e8e0c28fc3a046a4a2dc1ee29d'


def test_hash_csv(treedb, expected=HASH):
    assert treedb.hash_csv() == expected


def test_print_languoid_stats(capsys, treedb):
    assert treedb.print_languoid_stats() is None

    out, err = capsys.readouterr()
    assert not err
    assert out == '''\
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
'''
