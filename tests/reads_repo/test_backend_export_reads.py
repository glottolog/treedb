import sys

import pytest
import sqlalchemy as sa

from conftest import assert_file_size_between

QUERY_HASH = {'master': None,
              'v4.4': '',
              'v4.3-treedb-fixes': 'bf8af9e4840642f4622cec41bf3156afac75317740ff0eef1ac75ec1998d4f78',
              'v4.2.1': '25222b4feb2d89b4edaeecc546280a059ae6ba69da961d56ee4a387ba3b81fc0',
              'v4.2': '0623ea039d105309ccda567541c5fa8deba44c542d89169bff5df2e50eb8cbcf',
              'v4.1': '55e9cab42b012048ae9f6c08353752fded7054bf531039979c6803ede54b95ac'}


def test_print_dataset(capsys, treedb):
    assert treedb.print_dataset() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith("git describe '")


@pytest.skip_slow
@pytest.mark.skipif(sys.version_info < (3, 7), reason='requires Python 3.7+')
def test_backup(treedb):
    path = treedb.engine.file_with_suffix('.backup.sqlite3')

    engine = treedb.backup(path.name)

    assert engine.url.database == path.name
    assert_file_size_between(path, 10, 200)

    # SQLiteEngineProxy
    engine = treedb.engine.__class__(engine, future=treedb.engine.future)

    assert engine.file_exists()

    assert engine.file_mtime()

    assert engine.file_size() == path.stat().st_size
    assert 10 <= engine.file_size(as_megabytes=True) <= 200

    assert len(engine.file_sha256()) == 64


@pytest.skip_slow
def test_dump_sql(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.dump_sql()

    assert path.name == f'treedb{suffix}.sql.gz'
    assert_file_size_between(path, 1, 20)


@pytest.skip_slow
def test_csv_zipfile(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.csv_zipfile()

    assert path.name == f'treedb{suffix}.zip'
    assert_file_size_between(path, 1, 20)


def test_print_rows(capsys, treedb):
    query = (sa.select(treedb.Languoid)
             .where(treedb.Languoid.iso639_3 == 'bsa'))

    format_ = '{id}: {name} ({level}) [{iso639_3}]'

    assert treedb.print_rows(query, format_=format_, verbose=True) is None

    out, err = capsys.readouterr()
    assert not err

    assert out == '''\
SELECT languoid.id, languoid.name, languoid.level, languoid.parent_id, languoid.hid, languoid.iso639_3, languoid.latitude, languoid.longitude 
FROM languoid 
WHERE languoid.iso639_3 = :iso639_3_1
abin1243: Abinomn (language) [bsa]
'''


def test_write_csv(treedb):
    expected = QUERY_HASH.get(pytest.FLAGS.glottolog_tag)
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.write_csv()

    assert path.name == f'treedb{suffix}.query.csv'
    assert_file_size_between(path, 1, 30)

    if expected is None:
        pass
    else:
        shasum = treedb.sha256sum(path)
        assert shasum == expected


def test_hash_csv(treedb):
    expected = QUERY_HASH.get(pytest.FLAGS.glottolog_tag)

    result = treedb.hash_csv()

    if expected is None:
        assert len(result) == 64
    else:
        assert result == expected
