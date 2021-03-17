# test_backend.py

import re
import sys

import sqlalchemy as sa

import pytest

import treedb as _treedb

MB = 2**20


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
def test_print_query_sql(capsys, query, pretty, expected):
    assert _treedb.print_query_sql(query, pretty=pretty) is None

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


def test_print_schema(capsys):
    assert _treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip().startswith('CREATE TABLE __dataset__ (')


@pytest.mark.skipif(sys.version_info < (3, 7), reason='requires Python 3.7+')
def test_backup(treedb):
    path = treedb.engine.file_with_suffix('.backup.sqlite3')

    engine = treedb.backup(path.name)

    assert engine.url.database == path.name
    assert path.exists()
    assert path.is_file()
    assert 10 * MB <= path.stat().st_size <= 200 * MB

    # SQLiteEngineProxy
    engine = treedb.engine.__class__(engine, future=treedb.engine.future)

    assert engine.file_exists()

    assert engine.file_mtime()

    assert engine.file_size() == path.stat().st_size
    assert 10 <= engine.file_size(as_megabytes=True) <= 200

    assert len(engine.file_sha256()) == 64


def test_dump_sql(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.dump_sql()

    assert path.name == f'treedb{suffix}.sql.gz'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 20 * MB


def test_export(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.export()

    assert path.name == f'treedb{suffix}.zip'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 20 * MB
