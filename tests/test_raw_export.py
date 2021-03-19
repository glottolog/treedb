# test_raw_export.py

import pytest

RAW_CSV_SHA256 = {'v4.1': ('963163852e7f4ee34b516bc459bdbb90'
                           '8f2f4aab64bda58087a1a23a731921fd'),
                  'v4.2': ('e2ac065f5ce73af2165eb401831b53e0'
                           '494d8b25c4aa360fef879322b46f5f72'),
                  'v4.2.1': ('ab9d4339f3c0fa3acb0faf0f7306dc54'
                             '09640ecd46e451de9a76445519f5157e'),
                  'v4.3-treedb-fixes':
                          ('1ef6923a94d19c708fd0e7ae87b6ee24'
                           'c69d1d82fa9f81b16eaa5067e61ab1b6')}

MB = 2**20


pytestmark = pytest.FLAGS.skip_exclude_raw


def test_print_stats(capsys, treedb_raw):
    assert treedb_raw.print_stats() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip()


def test_write_raw_csv(treedb_raw):
    import treedb

    expected = RAW_CSV_SHA256.get(pytest.FLAGS.glottolog_tag)
    suffix = '-memory' if treedb.ENGINE.file is None else ''

    path = treedb_raw.write_raw_csv()

    assert path.name == f'treedb{suffix}.raw.csv.gz'
    assert path.exists()
    assert path.is_file()
    assert 1 * MB <= path.stat().st_size <= 100 * MB

    if expected is None:
        pass
    else:
        shasum = treedb._tools.sha256sum(path)
        assert shasum == expected
