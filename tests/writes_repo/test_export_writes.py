import pytest

FILES_WRITTEN = {'raw': {'master': 0,
                           'v4.3-treedb-fixes': 0,
                           'v4.2.1': 0,
                           'v4.2': 0,
                           'v4.1': 0},
                 'tables': {'master': 0,
                             # replace 'Country Name (ID)' with 'ID'
                             'v4.3-treedb-fixes': 8_528,
                             'v4.2.1': 8_528,
                             'v4.2': 8_528,
                             'v4.1': 8_540}}


@pytest.mark.parametrize('source', [
    pytest.param('raw',
                 marks=pytest.mark.xfail(reason='TODO',
                                         raises=NotImplementedError)),
    'tables'])
def test_write_files(treedb, source):
    expected = FILES_WRITTEN[source].get(pytest.FLAGS.glottolog_tag)

    files_written = treedb.write_files(source=source,
                                       dry_run=True,
                                       require_nwritten=expected)
    if expected is not None:
        assert files_written == expected
    else:
        assert files_written > -1
