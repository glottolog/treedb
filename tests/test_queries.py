# test_queries.py

import pytest

QUERY_HASH = {'v4.1': ('3842d6fd47d534486ed15531e035ee4b'
                       'ed8c3a31fd44b36ff565983ba5f1114e'),
              'v4.2': ('a1ff86daa0f4ca7854407ba31ae4d7839'
                       'bcdc1cbafd92b720d01775094862564'),
              'v4.2.1': ('90d180f3c2beff26763359945f68e8ac'
                         '94faf4379d1e36807ee9d7b835821ae8')}

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
'''}

STATS['v4.2'] = STATS['v4.2.1']

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
    expected = QUERY_HASH.get(pytest.FLAGS.glottolog_tag)
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_csv()

    assert path.name == f'treedb{suffix}.query.csv'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 30 * MB

    if expected is None:
        pass
    else:
        assert treedb.tools.sha256sum(path) == expected


def test_hash_csv(treedb):
    expected = QUERY_HASH.get(pytest.FLAGS.glottolog_tag)

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

    assert path.name == (f'treedb{suffix}'
                         f'.languoids-json_query{raw_suffix}.csv.gz')
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 100 * MB


def test_print_languoid_stats(capsys, treedb):
    expected = STATS.get(pytest.FLAGS.glottolog_tag)

    assert treedb.print_languoid_stats() is None

    out, err = capsys.readouterr()
    assert not err

    if expected is None:
        assert out.strip()
    else:
        assert out == expected


@pytest.mark.parametrize('kwargs, expected_first', [
    ({'parent_level': 'top', 'child_level': 'language'},
     ('abkh1242', ['abaz1241', 'abkh1244',
                               'adyg1241', 'kaba1278',
                               'ubyk1235'])),
])
def test_iterdescendants(treedb, kwargs, expected_first):
    pairs = treedb.iterdescendants(**kwargs)
    first = next(pairs)

    assert first == expected_first
