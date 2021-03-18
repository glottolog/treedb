# test_raw_tools.py

import pytest


pytestmark = pytest.FLAGS.skip_exclude_raw


def test_print_stats(capsys, treedb_raw):
    assert treedb_raw.print_stats() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip()
