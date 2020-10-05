# test_queries.py

import itertools
import json

import pytest

QUERY_HASH = {'v4.1': ('55e9cab42b012048ae9f6c08353752fd'
                       'ed7054bf531039979c6803ede54b95ac'),
              'v4.2': ('0623ea039d105309ccda567541c5fa8d'
                       'eba44c542d89169bff5df2e50eb8cbcf'),
              'v4.2.1': ('25222b4feb2d89b4edaeecc546280a05'
                         '9ae6ba69da961d56ee4a387ba3b81fc0')}

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
        shasum = treedb.tools.sha256sum(path)
        assert shasum == expected


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


def test_write_json_lines(capsys, treedb, n=100):
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb.write_json_lines()

    assert path.name == f'treedb{suffix}.languoids.jsonl'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 200 * MB

    with path.open(encoding='utf-8') as f:
        head = list(itertools.islice(f, n))

        assert head
        assert len(head) == n

        for line in head:
            item = json.loads(line)
            assert isinstance(item, dict)
            assert item

            path = item['path']
            assert isinstance(path, list)
            assert all(isinstance(p, str) for p in path)
            assert path
            assert all(path)

            languoid = item['languoid']
            assert isinstance(languoid, dict)
            assert languoid
            assert languoid['id']
            assert languoid['parent_id'] is None or languoid['parent_id']
            assert languoid['level'] in ('family', 'language', 'dialect')
            assert languoid['name']

    out, err = capsys.readouterr()
    assert not out
    assert not err


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
