import io
import json

import pytest

from helpers import (pairwise,
                     get_assert_head,
                     assert_nonempty_string,
                     assert_nonempty_dict,
                     assert_file_size_between,
                     assert_valid_languoids)

PREFIX = 'path_languoid:path:sha256:'

PREFIX_ID = 'path_languoid:id:sha256:'

CHECKSUM = {'master': None,
            'v4.7': 'bd2be4c0a0463c85a01e287cf12f1d73041cc775f26d6a5d69a419ed0f7aad09',
            'v4.6': '3e07ebd1d58f84604dab5f717dfcd2745b8dd37adbecab61a6bfdcb4b9e85d1c',
            'v4.5': '308e0e142683b5365d36dacb57cc22926d60c9d4b061eba6d58829b75b8843cb',
            'v4.4': '289247f73d5bf57f0a7dc8e0e2473b5625ace605dbad57b3c06fe93063d47599',
            'v4.3-treedb-fixes': '54b468c7310fdd958b2b17fe439ee47c00d211498b405a5bd74b2920f68e3969',
            'v4.2.1': '9e19d66c95a43f595a8ea0b72ba6e7e293e02faf66978b63dda8ddba7d37e3f6',
            'v4.1': '9b795566bd7f5ccb10e0cb4f5e5be10b5ccce496d9816728b904f41b75cdd55a'}

STATS = {'master': None,
         'v4.7': '''\
26,416 languoids
   245 families
   184 isolates
   429 roots
 8,572 languages
 4,425 subfamilies
13,174 dialects
 7,636 Spoken L1 Languages
   215 Sign Language
   121 Unclassifiable
    84 Pidgin
    68 Unattested
    32 Artificial Language
     9 Mixed Language
    15 Speech Register
 8,180 All
   392 Bookkeeping
''',
         'v4.6': '''\
26,285 languoids
   245 families
   182 isolates
   427 roots
 8,565 languages
 4,388 subfamilies
13,087 dialects
 7,628 Spoken L1 Languages
   215 Sign Language
   121 Unclassifiable
    84 Pidgin
    68 Unattested
    32 Artificial Language
    10 Mixed Language
    15 Speech Register
 8,173 All
   392 Bookkeeping
''',
         'v4.5': '''\
26,101 languoids
   245 families
   182 isolates
   427 roots
 8,549 languages
 4,348 subfamilies
12,959 dialects
 7,616 Spoken L1 Languages
   210 Sign Language
   121 Unclassifiable
    83 Pidgin
    68 Unattested
    31 Artificial Language
    11 Mixed Language
    15 Speech Register
 8,155 All
   394 Bookkeeping
''',
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


def test_print_languoid_stats(pytestconfig, capsys, treedb):
    expected = STATS.get(pytestconfig.option.glottolog_tag)

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


@pytest.mark.parametrize(
    'source',
    ['files',
     pytest.param('raw', marks=pytest.mark.raw),
     'tables'],
    ids=lambda x: f'source={x}')
def test_checksum(pytestconfig, treedb, source):
    expected = CHECKSUM.get(pytestconfig.option.glottolog_tag)

    result = treedb.checksum()

    if expected is None:
        assert result.startswith(PREFIX)
        assert len(result) - len(PREFIX) == 64
    else:
        assert result == PREFIX + expected


def test_write_json_lines_checksum(pytestconfig, treedb):
    expected = CHECKSUM.get(pytestconfig.option.glottolog_tag)

    with io.StringIO() as buf:
        treedb.write_languoids(buf)
        value = buf.getvalue()

    result = treedb.sha256sum(value, hash_file_string=True)

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected


@pytest.mark.parametrize('suffix',
                         ['.jsonl', '.jsonl.gz'],
                         ids=lambda x: f'suffix={x}')
def test_write_json_lines(pytestconfig, capsys, treedb, suffix, n=100):
    name_suffix = '-memory' if treedb.engine.file is None else ''
    name_suffix += pytestconfig.option.file_engine_tag
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

    expected_checksum = CHECKSUM.get(pytestconfig.option.glottolog_tag)
    if expected_checksum is not None:
        assert treedb.sha256sum(filepath) == expected_checksum


def test_write_json_lines_indent(tmp_path, treedb, limit=2):
    target = tmp_path / 'languoids.json.txt'
    with pytest.warns(UserWarning,
                      match=r'non-canonical JSON Lines format from indent 2'):
        path, n_written = treedb.write_languoids(target, pretty=True,
                                                 limit=limit)

    assert path == target
    assert n_written

    text = path.read_text(encoding='utf-8')
    assert text.startswith('{\n  "__path__": [')
    assert text.endswith('\n    }\n  }\n}\n')


@pytest.mark.pandas
@pytest.mark.parametrize(
    'source',
    ['files',
     pytest.param('raw', marks=pytest.mark.raw),
     'tables'],
    ids=lambda x: f'source={x}')
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


xfail_master_unnormalized = pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized master',
                                                            raises=AssertionError)


xfail_empty_altnames_elcat = pytest.mark.xfail_glottolog_tag('v4.5', reason='empty altnames',
                                                             # https://github.com/glottolog/glottolog/pull/79
                                                             raises=AssertionError)


xfail_float_normalization = pytest.mark.xfail_glottolog_tag('v4.1', reason='float normalization',
                                                            # https://github.com/glottolog/glottolog/pull/495
                                                            raises=AssertionError)


@pytest.mark.parametrize(
    'kwargs',
    [pytest.param([{'source': 'files'},
                   {'source': 'raw'},
                   {'source': 'tables'}],
                  id='files, raw, tables',
                  marks=[xfail_master_unnormalized,
                         xfail_empty_altnames_elcat,
                         xfail_float_normalization]),
     pytest.param([{'source': 'raw', 'order_by': 'id'},
                   {'source': 'tables', 'order_by': 'id'}],
                  id='raw(order_by=id), tables(order_by=id)',
                  marks=[xfail_master_unnormalized,
                         xfail_float_normalization]),
     pytest.param([{'source': 'files', 'limit': 100,
                    'expected_prefix': 'path_languoid:path[limit=100]:sha256:'},
                   {'source': 'raw', 'order_by': 'file', 'limit': 100,
                    'expected_prefix': 'path_languoid:file[limit=100]:sha256:'}],
                  id='files(limit=100), raw(order_by=file, limit=100)',
                  marks=[xfail_empty_altnames_elcat])])
def test_checksum_equivalence(pytestconfig, treedb, kwargs):
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
    glottolog repository md.ini files (see xfails above).

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
            # --exclude-raw: filter out raw sources instead of skipping
            if pytestconfig.option.exclude_raw and kw.get('source') == 'raw':
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

    for (c, _, _, cur), (n, _, _, nxt) in pairwise(results):
        cur, nxt = (s.rpartition(':')[2] for s in (cur, nxt))
        assert cur == nxt, f'checksum(**{c!r}) == checksum(**{n!r})'
