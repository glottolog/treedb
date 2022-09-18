import io
import re

import pytest
import sqlalchemy as sa

import treedb as _treedb


def test_print_schema(capsys, bare_treedb):
    assert bare_treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err

    assert out.strip().startswith('CREATE TABLE __dataset__ (')


@pytest.mark.parametrize(
    'query, pretty, expected',
    [pytest.param(sa.select(_treedb.Languoid), False,
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
                  id='query=Languoid, pretty', marks=pytest.mark.sqlparse),
     pytest.param(None, False, None, id='query=None'),
     pytest.param(None, True, None, id='query=None, pretty')])
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


def test_print_rows_pretty(bare_treedb, encoding='utf-8'):
    value = dict.fromkeys(f'key_{i}' for i in range(10))
    value = _treedb.backend.json_object(sort_keys_=False,
                                        load_json_=True,
                                        label_='record',
                                        **value)
    query = sa.select(value)
    with io.StringIO() as f:
        bare_treedb.print_rows(query, pretty=True, file=f)
        result = f.getvalue()
    assert result == '''\
{'record': {'key_0': None,
            'key_1': None,
            'key_2': None,
            'key_3': None,
            'key_4': None,
            'key_5': None,
            'key_6': None,
            'key_7': None,
            'key_8': None,
            'key_9': None}}
'''
