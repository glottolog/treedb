import re

import pytest


@pytest.mark.parametrize('table, expected', [
    pytest.param('__dataset__', re.compile(r'CREATE TABLE __dataset__ \(\n'
                                           r'.+\n'
                                           r'0', re.DOTALL),
                 id='table=__dataset__'),
])
def test_print_table_sql(capsys, empty_treedb, table, expected):
    assert empty_treedb.print_table_sql(table) is None

    out, err = capsys.readouterr()
    assert not err

    assert expected.fullmatch(out.strip())


def test_select_tables_nrows(empty_treedb):
    query = empty_treedb.select_tables_nrows()
    with empty_treedb.connect() as conn:
        rows = conn.execute(query).all()

    assert rows

    for table, nrows in rows:
        assert isinstance(table, str)
        assert table

        assert isinstance(nrows, int)
        assert nrows == 0


def test_select_views(empty_treedb):
    query = empty_treedb.backend.sqlite_master.select_views()

    with empty_treedb.connect() as conn:
        names = conn.execute(query).scalars().all()

    assert names == ['example', 'path_languoid', 'stats']
