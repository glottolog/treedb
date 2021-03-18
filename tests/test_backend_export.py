# test_backend_export.py

import sys

import pytest

MB = 2**20


@pytest.skip_slow
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


@pytest.skip_slow
def test_dump_sql(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.dump_sql()

    assert path.name == f'treedb{suffix}.sql.gz'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 20 * MB


@pytest.skip_slow
def test_export(treedb):
    suffix = '-memory' if treedb.engine.file is None else ''

    path = treedb.export()

    assert path.name == f'treedb{suffix}.zip'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 20 * MB
