import pytest

FILES_WRITTEN = {'master': 0,
                 'v4.3-treedb-fixes': 0,
                 'v4.2.1': 0,
                 'v4.2': 0,
                 'v4.1': 0}


@pytest.mark.xfail(reason='TODO', raises=KeyError)
@pytest.mark.xfail(bool(pytest.FLAGS.file_engine),
                   reason="FIXME: NameError: name 'open' is not defined'",
                   raises=KeyError, run=False)
def test_write_files(treedb_raw):
    expected = FILES_WRITTEN.get(pytest.FLAGS.glottolog_tag)

    files_written = treedb_raw.raw.write_files(dry_run=True,
                                               require_nwritten=expected,
                                               bind=treedb_raw.engine)
    if expected is not None:
        assert files_written == expected
    else:
        assert files_written > -1
