import re

import pytest


@pytest.mark.parametrize('table, expected', [
    ('__dataset__', re.compile(r'CREATE TABLE __dataset__ \(\n'
                               r'.+\n1', re.DOTALL)),
])
def test_print_table_sql(capsys, treedb, table, expected):
    assert treedb.print_table_sql(table) is None

    out, err = capsys.readouterr()
    assert not err
    assert expected.fullmatch(out.strip())


def test_select_tables_nrows(treedb):
    query = treedb.select_tables_nrows()
    rows = query.execute().fetchall()

    assert rows
    for table, nrows in rows:
        assert table
        assert isinstance(nrows, int)
        assert nrows


def test_select_views(treedb):
    query = treedb.sqlite_master.select_views()
    names = [n for n, in query.execute()]

    assert names == ['example', 'path_json', 'stats']
