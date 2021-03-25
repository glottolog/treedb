import re
import sys

import pytest
import sqlalchemy as sa

from conftest import assert_file_size_between

import treedb as _treedb

QUERY_HASH = {'v4.1': ('55e9cab42b012048ae9f6c08353752fd'
                       'ed7054bf531039979c6803ede54b95ac'),
              'v4.2': ('0623ea039d105309ccda567541c5fa8d'
                       'eba44c542d89169bff5df2e50eb8cbcf'),
              'v4.2.1': ('25222b4feb2d89b4edaeecc546280a05'
                         '9ae6ba69da961d56ee4a387ba3b81fc0'),
              'v4.3-treedb-fixes':
                      ('bf8af9e4840642f4622cec41bf3156af'
                       'ac75317740ff0eef1ac75ec1998d4f78')}


def test_print_dataset(capsys, treedb):
    assert treedb.print_dataset() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith("git describe '")


def test_print_schema(capsys, bare_treedb):
    assert bare_treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip().startswith('CREATE TABLE __dataset__ (')


@pytest.mark.parametrize('query, pretty, expected', [
    (sa.select(_treedb.Languoid), False, ('SELECT languoid.id,'
                                          ' languoid.name,'
                                          ' languoid.level,'
                                          ' languoid.parent_id,'
                                          ' languoid.hid,'
                                          ' languoid.iso639_3,'
                                          ' languoid.latitude,'
                                          ' languoid.longitude \n'
                                          'FROM languoid\n')),
    (sa.select(_treedb.Languoid), True, ('SELECT languoid.id,\n'
                                         '       languoid.name,\n'
                                         '       languoid.level,\n'
                                         '       languoid.parent_id,\n'
                                         '       languoid.hid,\n'
                                         '       languoid.iso639_3,\n'
                                         '       languoid.latitude,\n'
                                         '       languoid.longitude\n'
                                         'FROM languoid\n')),
    (None, False, None),
    (None, True, None)
])
def test_print_query_sql(capsys, bare_treedb, query, pretty, expected):
    assert bare_treedb.print_query_sql(query, pretty=pretty) is None

    out, err = capsys.readouterr()
    assert not err

    if expected is None:
        assert out.strip()
    elif pretty and _treedb.backend.sqlparse is None:
        norm_out, norm_exp = (re.sub(r'\s{1,}', r' ', s).strip()
                              for s in (out, expected))
        assert norm_out == norm_exp
    else:
        assert out == expected


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
