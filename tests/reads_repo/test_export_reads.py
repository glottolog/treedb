import io
import json

import pytest

from conftest import (pairwise,
                      get_assert_head,
                      assert_nonempty_string,
                      assert_nonempty_dict,
                      assert_file_size_between,
                      assert_valid_languoids)

PREFIX = 'path_languoid:path:sha256:'

PREFIX_ID = 'path_languoid:id:sha256:'

CHECKSUM = {'master': None,
            'v4.3-treedb-fixes': '54b468c7310fdd958b2b17fe439ee47c00d211498b405a5bd74b2920f68e3969',
            'v4.2.1': '9e19d66c95a43f595a8ea0b72ba6e7e293e02faf66978b63dda8ddba7d37e3f6',
            'v4.2': '91756e0e9150872e71a1c27419a302d229f4b497f5f629d1d98fc40305f8e5ea',
            'v4.1': '9b795566bd7f5ccb10e0cb4f5e5be10b5ccce496d9816728b904f41b75cdd55a'}

STATS = {'master': None,
         'v4.4': '''\
25,900 languoids
   245 families
   182 isolates
   427 roots
 8,533 languages
 4,326 subfamilies
12,796 dialects
 7,613 Spoken L1 Languages
   202 Sign Language
   122 Unclassifiable
    83 Pidgin
    68 Unattested
    31 Artificial Language
    11 Mixed Language
    14 Speech Register
 8,144 All
   389 Bookkeeping
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
        'v4.1': '''\
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

STATS['v4.2'] = STATS['v4.2.1']

MB = 2**20


def test_print_languoid_stats(capsys, treedb):
    expected = STATS.get(pytest.FLAGS.glottolog_tag)

    assert treedb.print_languoid_stats() is None

    out, err = capsys.readouterr()
    assert not err

    if expected is None:
        assert out.strip()
    else:
        assert out == expected


def test_iterlanguoids(bare_treedb, n=100):
    items = bare_treedb.iterlanguoids()
    assert_valid_languoids(items, n=n)


@pytest.mark.parametrize('source', [
    'files',
    pytest.param('raw', marks=pytest.FLAGS.skip_exclude_raw),
    'tables'])
def test_checksum(treedb, source):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)

    result = treedb.checksum()

    if expected is None:
        assert result.startswith(PREFIX)
        assert len(result) - len(PREFIX) == 64
    else:
        assert result == PREFIX + expected


def test_write_json_lines_checksum(treedb):
    expected = CHECKSUM.get(pytest.FLAGS.glottolog_tag)

    with io.StringIO() as buf:
        treedb.write_languoids(buf)
        value = buf.getvalue()

    result = treedb.sha256sum(value, hash_file_string=True)

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected


@pytest.mark.parametrize('suffix', ['.jsonl', '.jsonl.gz'])
def test_write_json_lines(capsys, treedb, suffix, n=100):
    name_suffix = '-memory' if treedb.engine.file is None else ''
    args = ([f'treedb{name_suffix}.languoids{suffix}'] if suffix != 'jsonl.gz'
            else [])

    filepath, _ = treedb.write_languoids(*args)

    assert filepath.name == f'treedb{name_suffix}.languoids{suffix}'
    assert_file_size_between(filepath, 1, 200)

    if filepath.name.endswith('.jsonl'):
        with filepath.open(encoding='utf-8') as f:
            for line in get_assert_head(f, n=n):
                item = json.loads(line)
                assert_nonempty_dict(item)

                path = item['__path__']
                assert isinstance(path, list)
                assert all(isinstance(p, str) for p in path)
                assert path
                assert all(path)

                languoid = item['languoid']
                assert_nonempty_dict(languoid)

                for key in ('id', 'level', 'name'):
                    assert_nonempty_string(languoid[key])

                assert languoid['parent_id'] or languoid['parent_id'] is None
                assert languoid['level'] in ('family', 'language', 'dialect')

    out, err = capsys.readouterr()
    assert not out
    assert not err

    expected_checksum = CHECKSUM.get(pytest.FLAGS.glottolog_tag)
    if expected_checksum is not None:
        assert treedb.sha256sum(filepath) == expected_checksum


@pytest.mark.parametrize('source', [
    'files',
    pytest.param('raw', marks=pytest.FLAGS.skip_exclude_raw),
    'tables'])
def test_pd_read_languoids(treedb, source, limit=1_000):
    df = treedb.pd_read_languoids(source=source)

    if treedb.backend.pandas.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        assert df.index.name == 'id'
        assert list(df.columns) == ['path', 'languoid']
        assert df.index.is_unique
        df.info(memory_usage='deep')


@pytest.mark.skipif(pytest.FLAGS.glottolog_tag == 'v4.1',
                    reason='requires https://github.com/glottolog/glottolog/pull/495')
@pytest.FLAGS.skip_exclude_raw
@pytest.mark.parametrize('kwargs', [
    [{'source': 'files'},
     {'source': 'raw'},
     {'source': 'tables'}],
    [{'source': 'raw', 'order_by': 'id'},
     {'source': 'tables', 'order_by': 'id'}],
    [{'source': 'files', 'limit': 10,
      'expected_prefix': 'path_languoid:path[limit=10]:sha256:'},
     {'source': 'raw', 'order_by': 'file', 'limit': 10,
      'expected_prefix': 'path_languoid:file[limit=10]:sha256:'}]])
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
    def iterchecksums(kwargs):
        for kw in kwargs:
            expected_prefix = kw.pop('expected_prefix', None)
            if kw.get('source') == 'raw' and pytest.FLAGS.exclude_raw:
                pytest.skip('skipped by --exclude-raw')
                continue
            checksum = treedb.checksum(**kw)
            prefix, colon, hexdigest = checksum.rpartition(':')
            prefix += colon
            yield kw, expected_prefix, prefix, hexdigest

    results = list(iterchecksums(kwargs))

    for kw, expected_prefix, prefix, hexdigest in results:
        assert len(hexdigest) == 64
        if expected_prefix is None:
            expected_prefix = PREFIX_ID if kw.get('order_by') == 'id' else PREFIX
        assert prefix == expected_prefix

    if pytest.FLAGS.glottolog_tag in ('v4.1',
                                      'v4.2', 'v4.2.1',
                                      'v4.3-treedb-fixes'):
        pytest.xfail('format change: minimal countries')

    for (c, _, _, cur), (n, _, _, nxt) in pairwise(results):
        cur, nxt = (s.rpartition(':')[2] for s in (cur, nxt))
        assert cur == nxt, f'checksum(**{c!r}) == checksum(**{n!r})'
