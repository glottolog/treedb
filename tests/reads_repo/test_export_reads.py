import io
import json
import sqlite3

import pytest
import sqlalchemy as sa

from helpers import (pairwise,
                     get_assert_head,
                     assert_nonempty_string,
                     assert_nonempty_dict,
                     assert_file_size_between,
                     assert_valid_languoids)

PREFIX = 'path_languoid:path:sha256:'

PREFIX_ID = 'path_languoid:id:sha256:'

CHECKSUM = {'master': None,
            'v5.3': '82d08f00d3596d437a66084f1d97fb68608df4a72d93278a8a8442e053e795ba',
            'v5.2.1': '0f0b5cb36fa2b9c1c8ed087f8bff0e46b7fc37f0c6ad14fc65139486db88c3dd',
            'v5.2': 'c80bc501cf1bef9f3f9449ffa8a6a60d0a4082cfa74f82768490e698dc3e3c70',
            'v5.1': 'e37d43e99deb5fb5055916b29c009c43fafffc2c60c4e9fc027b34d16336d856',
            'v5.0': '41ad0e4b1ef0569c9699f1591f1a3fa9de64bee17cfc8b968330d96eff4d3d84'}

STATS = {'master': None,
         'v5.3': '''\
27,177 languoids
   246 families
   183 isolates
   429 roots
 8,618 languages
 4,607 subfamilies
13,706 dialects
 7,674 Spoken L1 Languages
   227 Sign Language
   128 Unclassifiable
    87 Pidgin
    68 Unattested
    31 Artificial Language
     4 Mixed Language
    19 Speech Register
 8,238 All
   380 Bookkeeping
''',
         'v5.2.1': '''\
27,037 languoids
   247 families
   182 isolates
   429 roots
 8,612 languages
 4,585 subfamilies
13,593 dialects
 7,675 Spoken L1 Languages
   227 Sign Language
   120 Unclassifiable
    87 Pidgin
    68 Unattested
    31 Artificial Language
     8 Mixed Language
    15 Speech Register
 8,231 All
   381 Bookkeeping
''',
         'v5.2': '''\
27,034 languoids
   246 families
   184 isolates
   430 roots
 8,612 languages
 4,583 subfamilies
13,593 dialects
 7,675 Spoken L1 Languages
   227 Sign Language
   120 Unclassifiable
    87 Pidgin
    68 Unattested
    31 Artificial Language
     8 Mixed Language
    15 Speech Register
 8,231 All
   381 Bookkeeping
''',
         'v5.1': '''\
26,953 languoids
   247 families
   183 isolates
   430 roots
 8,605 languages
 4,547 subfamilies
13,554 dialects
 7,667 Spoken L1 Languages
   227 Sign Language
   120 Unclassifiable
    87 Pidgin
    68 Unattested
    31 Artificial Language
     8 Mixed Language
    15 Speech Register
 8,223 All
   382 Bookkeeping
''',
         'v5.0': '''\
26,879 languoids
   246 families
   184 isolates
   430 roots
 8,604 languages
 4,522 subfamilies
13,507 dialects
 7,665 Spoken L1 Languages
   227 Sign Language
   120 Unclassifiable
    84 Pidgin
    68 Unattested
    31 Artificial Language
     9 Mixed Language
    15 Speech Register
 8,219 All
   385 Bookkeeping
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


def test_sqlite_json_3_53_fpdigits(treedb):
    # languoids/tree/pama1250/aran1266/thur1253/core1260/unun9967/ngad1257/md.ini
    longitude = 139.08123163651
    result = treedb.scalar(sa.func.json(longitude))
    assert isinstance(result, str)

    # https://www.sqlite.org/floatingpoint.html#*fpdigits
    if sqlite3.sqlite_version_info < (3, 52):
        assert result == '139.08123163651'
        assert result == str(longitude)
    else:
        # https://www.sqlite.org/releaselog/3_53_0.html
        assert result == '139.08123163651001'
        assert result.startswith(str(longitude))
        assert result != str(longitude)


@pytest.mark.parametrize(
    'source',
    ['files',
     pytest.param('raw', marks=pytest.mark.raw),
     'tables'],
    ids=lambda x: f'source={x}')
def test_checksum(pytestconfig, treedb, source):
    expected = CHECKSUM.get(pytestconfig.option.glottolog_tag)

    result = treedb.checksum(source)

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


xfail_master_unnormalized = pytest.mark.xfail_glottolog_tag(
    'master', reason='possibly unnormalized master',
    raises=AssertionError)


@pytest.mark.parametrize(
    'kwargs',
    [pytest.param([{'source': 'files'},
                   {'source': 'raw'},
                   {'source': 'tables'}],
                  id='files, raw, tables',
                  marks=[xfail_master_unnormalized]),
     pytest.param([{'source': 'raw', 'order_by': 'id'},
                   {'source': 'tables', 'order_by': 'id'}],
                  id='raw(order_by=id), tables(order_by=id)',
                  marks=[xfail_master_unnormalized]),
     pytest.param([{'source': 'files', 'limit': 100,
                    'expected_prefix': 'path_languoid:path[limit=100]:sha256:'},
                   {'source': 'raw', 'order_by': 'file', 'limit': 100,
                    'expected_prefix': 'path_languoid:file[limit=100]:sha256:'}],
                  id='files(limit=100), raw(order_by=file, limit=100)')])
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
