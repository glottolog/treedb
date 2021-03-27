import pytest

FILES_WRITTEN = {'master': 0,
                 # replace 'Country Name (ID)' with 'ID'
                 'v4.3-treedb-fixes': 8_528,
                 'v4.2.1': 8_528,
                 'v4.2': 8_528,
                 'v4.1': 8_540}


def test_write_files(treedb):
    expected = FILES_WRITTEN.get(pytest.FLAGS.glottolog_tag)

    files_written = treedb.write_files(dry_run=True, require_nwritten=expected)
    if expected is not None:
        assert files_written == expected
    else:
        assert files_written > -1
