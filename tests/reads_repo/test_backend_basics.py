def test_print_versions(capsys, bare_treedb):
    assert bare_treedb.print_versions() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith('treedb version: ')
