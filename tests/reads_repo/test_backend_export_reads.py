import pytest
import sqlalchemy as sa

from helpers import assert_file_size_between

QUERY_HASH = {'master': None,
              'v5.2': 'ac5712e082e4bf4c4b341d232a4309c5a968b9f93d1c23b9cd4a1ae9faed5197',
              'v5.1': 'c235e4b709b009a31f5a8eace0edeff02b08546f9f81b90139d017723fb4fa65',
              'v5.0': 'f20f1bd9e397f614b24276c189f072fbe28be8c62b6d5f3011546735aecb5bc6',
              'v4.8': '705b1e4e30c35c659e536f0b41dc2c4197c72037b2c8eec7af20c3bf9ef4c992',
              'v4.7': 'e1054be01c4e17150e875d1cab40c754c0e1aeb3b5343a36c5771d3587115537',
              'v4.6': '4479476b397fa7dfbfd560a4bef5be06513ddc54f7c103d1f565e3a26404a90f',
              'v4.5': 'b36b5cdc7508b8c2843a3c93ff536ead9b4b63cdf80679f70993a5dd524a8926',
              'v4.4': '224691678e1f2e18406d6dd1a278e062c683ac12ec2acf57d501931d3661142e',
              'v4.3-treedb-fixes': 'bf8af9e4840642f4622cec41bf3156afac75317740ff0eef1ac75ec1998d4f78',
              'v4.2.1': '25222b4feb2d89b4edaeecc546280a059ae6ba69da961d56ee4a387ba3b81fc0',
              'v4.1': '55e9cab42b012048ae9f6c08353752fded7054bf531039979c6803ede54b95ac'}


def test_print_dataset(capsys, treedb):
    assert treedb.print_dataset() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith("git describe '")


@pytest.mark.slow
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

    out, err = capsys.readouterr()
    assert not err

    assert out == '''\
SELECT languoid.id, languoid.name, languoid.level, languoid.parent_id, languoid.hid, languoid.iso639_3, languoid.latitude, languoid.longitude 
FROM languoid 
WHERE languoid.iso639_3 = :iso639_3_1
abin1243: Abinomn (language) [bsa]
'''  # noqa: E501,W291


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
