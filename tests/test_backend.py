import pytest

import treedb


@pytest.mark.parametrize('query, pretty, expected', [
    (treedb.select([treedb.Languoid]), False, ('SELECT languoid.id,'
                                               ' languoid.name,'
                                               ' languoid.level,'
                                               ' languoid.parent_id,'
                                               ' languoid.hid,'
                                               ' languoid.iso639_3,'
                                               ' languoid.latitude,'
                                               ' languoid.longitude \n'
                                               'FROM languoid\n')),
    (treedb.select([treedb.Languoid]), True, ('SELECT languoid.id,\n'
                                              '       languoid.name,\n'
                                              '       languoid.level,\n'
                                              '       languoid.parent_id,\n'
                                              '       languoid.hid,\n'
                                              '       languoid.iso639_3,\n'
                                              '       languoid.latitude,\n'
                                              '       languoid.longitude\n'
                                              'FROM languoid\n')),
])
def test_print_query_sql(capsys, query, pretty, expected):
    assert treedb.print_query_sql(query, pretty=pretty) is None

    out, err = capsys.readouterr()
    assert not err
    assert out == expected


def test_print_schema(capsys):
    assert treedb.print_schema() is None

    out, err = capsys.readouterr()
    assert not err
    assert out.strip().startswith('CREATE TABLE __dataset__ (')
