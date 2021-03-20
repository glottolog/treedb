def test_print_versions(capsys, treedb):
    assert treedb.print_versions() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith('treedb version: ')
