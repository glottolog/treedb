import textwrap

import pytest
import sqlalchemy as sa

from helpers import assert_file_size_between

QUERY_HASH = {'master': None,
              'v5.3': '4c9711d7da62c565250bb473bfd0c493d752e37e04c00d61dcc06db625e7fdcf',
              'v5.2.1': 'ced50816870c5da047d4533187c30224786da02a2b7372140545a5a73cb98012',
              'v5.2': 'ac5712e082e4bf4c4b341d232a4309c5a968b9f93d1c23b9cd4a1ae9faed5197',
              'v5.1': 'c235e4b709b009a31f5a8eace0edeff02b08546f9f81b90139d017723fb4fa65',
              'v5.0': 'f20f1bd9e397f614b24276c189f072fbe28be8c62b6d5f3011546735aecb5bc6'}


def test_print_dataset(capsys, treedb):
    assert treedb.print_dataset() is None

    (out, err) = capsys.readouterr()
    assert not err

    assert out.startswith("git describe '")


@pytest.mark.slow
def test_backup(treedb):
    path = treedb.engine.file_with_suffix('.backup.sqlite3')

    engine = treedb.backup(path.name)

    assert engine.url.database == path.name
    assert_file_size_between(path, 10, 200)

    # SQLiteEngineProxy
    engine = treedb.engine.__class__(engine,)

    assert engine.file_exists()

    assert engine.file_mtime()

    assert engine.file_size() == path.stat().st_size
    assert 10 <= engine.file_size(as_megabytes=True) <= 200

    assert len(engine.file_sha256()) == 64


@pytest.mark.slow
def test_dump_sql(pytestconfig, treedb):
    suffix = '-memory' if treedb.engine.file is None else ''
    suffix += pytestconfig.option.file_engine_tag

    path = treedb.dump_sql()

    assert path.name == f'treedb{suffix}.sql.gz'
    assert_file_size_between(path, 1, 20)


@pytest.mark.slow
def test_csv_zipfile(pytestconfig, treedb):
    suffix = '-memory' if treedb.engine.file is None else ''
    suffix += pytestconfig.option.file_engine_tag

    path = treedb.csv_zipfile()

    assert path.name == f'treedb{suffix}.zip'
    assert_file_size_between(path, 1, 20)


def test_print_rows(capsys, treedb):
    query = (sa.select(treedb.Languoid)
             .where(treedb.Languoid.iso639_3 == 'bsa'))

    format_ = '{id}: {name} ({level}) [{iso639_3}]'

    assert treedb.print_rows(query, format_=format_, verbose=True) is None

    (out, err) = capsys.readouterr()
    assert not err

    expected = textwrap.dedent(
        '''
        SELECT languoid.id, languoid.name, languoid.level, languoid.parent_id, languoid.hid, languoid.iso639_3, languoid.latitude, languoid.longitude 
        FROM languoid 
        WHERE languoid.iso639_3 = :iso639_3_1
        abin1243: Abinomn (language) [bsa]
        ''').lstrip()  # noqa: E501,W291
    assert out == expected


def test_write_csv(pytestconfig, treedb):
    expected = QUERY_HASH.get(pytestconfig.option.glottolog_tag)
    suffix = '-memory' if treedb.engine.file is None else ''
    suffix += pytestconfig.option.file_engine_tag

    path = treedb.write_csv()

    assert path.name == f'treedb{suffix}.query.csv'
    assert_file_size_between(path, 1, 30)

    if expected is None:
        pass
    else:
        shasum = treedb.sha256sum(path)
        assert shasum == expected


def test_hash_csv(pytestconfig, treedb):
    expected = QUERY_HASH.get(pytestconfig.option.glottolog_tag)

    result = treedb.hash_csv()

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected
