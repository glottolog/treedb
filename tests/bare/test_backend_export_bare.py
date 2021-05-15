import re

import pytest
import sqlalchemy as sa

import treedb as _treedb


def test_print_schema(capsys, bare_treedb):
    assert bare_treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip().startswith('CREATE TABLE __dataset__ (')


@pytest.mark.parametrize('query, pretty, expected', [
    pytest.param(sa.select(_treedb.Languoid), False,
                 ('SELECT languoid.id,'
                  ' languoid.name,'
                  ' languoid.level,'
                  ' languoid.parent_id,'
                  ' languoid.hid,'
                  ' languoid.iso639_3,'
                  ' languoid.latitude,'
                  ' languoid.longitude \n'
                  'FROM languoid\n'),
                 id='query=Languoid'),
    pytest.param(sa.select(_treedb.Languoid), True,
                 ('SELECT languoid.id,\n'
                  '       languoid.name,\n'
                  '       languoid.level,\n'
                  '       languoid.parent_id,\n'
                  '       languoid.hid,\n'
                  '       languoid.iso639_3,\n'
                  '       languoid.latitude,\n'
                  '       languoid.longitude\n'
                  'FROM languoid\n'),
                 id='query=Languoid, pretty'),
    pytest.param(None, False, None, id='query=None'),
    pytest.param(None, True, None, id='query=None, pretty'),
])
def test_print_query_sql(capsys, bare_treedb, query, pretty, expected):
    assert bare_treedb.print_query_sql(query, pretty=pretty) is None

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
