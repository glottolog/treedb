# test_sqlite_master.py

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
    with treedb.connect() as conn:
        rows = conn.execute(query).all()

    assert rows

    for table, nrows in rows:
        assert isinstance(table, str)
        assert table

        assert isinstance(nrows, int)
        minimum = 0 if table == 'timespan' else 1
        assert minimum <= nrows <= 1_000_000


def test_select_views(treedb):
    query = treedb.sqlite_master.select_views()
    with treedb.connect() as conn:
        names = [n for n, in conn.execute(query)]

    assert names == ['example', 'path_json', 'stats']
