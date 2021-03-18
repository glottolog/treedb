# test_backend_tools.py

import re

import pytest
import sqlalchemy as sa

import treedb as _treedb


@pytest.mark.parametrize('query, pretty, expected', [
    (sa.select(_treedb.Languoid), False, ('SELECT languoid.id,'
                                          ' languoid.name,'
                                          ' languoid.level,'
                                          ' languoid.parent_id,'
                                          ' languoid.hid,'
                                          ' languoid.iso639_3,'
                                          ' languoid.latitude,'
                                          ' languoid.longitude \n'
                                          'FROM languoid\n')),
    (sa.select(_treedb.Languoid), True, ('SELECT languoid.id,\n'
                                         '       languoid.name,\n'
                                         '       languoid.level,\n'
                                         '       languoid.parent_id,\n'
                                         '       languoid.hid,\n'
                                         '       languoid.iso639_3,\n'
                                         '       languoid.latitude,\n'
                                         '       languoid.longitude\n'
                                         'FROM languoid\n')),
    (None, False, None),
    (None, True, None)
])
def test_print_query_sql(capsys, query, pretty, expected):
    assert _treedb.print_query_sql(query, pretty=pretty) is None

    out, err = capsys.readouterr()
    assert not err

    if expected is None:
        assert out.strip()
    elif pretty and _treedb.backend.sqlparse is None:
        norm_out, norm_exp = (re.sub(r'\s{1,}', r' ', s).strip()
                              for s in (out, expected))
        assert norm_out == norm_exp
    else:
        assert out == expected


def test_print_schema(capsys):
    assert _treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip().startswith('CREATE TABLE __dataset__ (')
