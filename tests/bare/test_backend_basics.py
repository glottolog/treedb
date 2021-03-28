import sqlalchemy as sa


def test_print_versions(capsys, bare_treedb):
    assert bare_treedb.print_versions() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.startswith('treedb version: ')


def test_scalar(bare_treedb):
    result = bare_treedb.scalar(sa.select(sa.func.sqlite_version()))

    assert result is not None
    assert isinstance(result, str)
    assert result
