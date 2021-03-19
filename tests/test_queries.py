# test_queries.py

import itertools

import pytest

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
''',
         'v4.2.1': '''\
25,228 languoids
   242 families
   187 isolates
   429 roots
 8,515 languages
 4,234 subfamilies
12,237 dialects
 7,604 Spoken L1 Languages
   196 Sign Language
   123 Unclassifiable
    81 Pidgin
    66 Unattested
    28 Artificial Language
    14 Mixed Language
    10 Speech Register
 8,122 All
   393 Bookkeeping
''',
        'v4.3-treedb-fixes': '''\
25,439 languoids
   244 families
   181 isolates
   425 roots
 8,516 languages
 4,265 subfamilies
12,414 dialects
 7,606 Spoken L1 Languages
   196 Sign Language
   123 Unclassifiable
    81 Pidgin
    67 Unattested
    29 Artificial Language
    12 Mixed Language
    10 Speech Register
 8,124 All
   392 Bookkeeping
'''}

STATS['v4.2'] = STATS['v4.2.1']


def test_print_languoid_stats(capsys, treedb):
    expected = STATS.get(pytest.FLAGS.glottolog_tag)

    assert treedb.print_languoid_stats() is None

    out, err = capsys.readouterr()
    assert not err

    if expected is None:
        assert out.strip()
    else:
        assert out == expected


@pytest.mark.parametrize('kwargs, expected_head', [
    ({'parent_level': 'top', 'child_level': 'language'},
     [('abin1243', []),
      ('abis1238', []),
      ('abkh1242', ['abaz1241', 'abkh1244',
                                'adyg1241', 'kaba1278',
                                'ubyk1235']),
      ('adai1235', [])]),
])
def test_iterdescendants(treedb, kwargs, expected_head):
    pairs = treedb.iterdescendants(**kwargs)
    head = list(itertools.islice(pairs, len(expected_head)))

    assert head == expected_head
