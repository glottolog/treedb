import io
import itertools
import sys

import sqlalchemy as sa

import pytest


@pytest.mark.xfail_glottolog_tag('v4.1', reason='no timespan in Glottolog v4.1',
                                 raises=sa.exc.NoResultFound)
def test_select_languoid_timespan(treedb):
    timespan = treedb.queries.select_languoid_timespan(as_json=False,
                                                       label='ts')
    select = (sa.select(treedb.Languoid, timespan)
              .where(timespan != sa.null()).limit(1))
    with treedb.connect() as conn:
        row = conn.execute(select).one()

    assert row.ts


@pytest.mark.parametrize(
    'kwargs, expected_head',
    [pytest.param({'parent_level': 'top', 'child_level': 'language'},
                  [('abin1243', []),
                   ('abis1238', []),
                   ('abkh1242', ['abaz1241', 'abkh1244',
                                             'adyg1241', 'kaba1278',
                                             'ubyk1235']),
                   ('adai1235', [])],
                  id='parent_level=top, child_level=language')])
def test_iterdescendants(treedb, kwargs, expected_head):
    pairs = treedb.iterdescendants(**kwargs)
    head = list(itertools.islice(pairs, len(expected_head)))

    assert head == expected_head


@pytest.mark.parametrize(
    'as_rows, sort_keys',
    [(True, False),
     (True, True),
     (False, False),
     (False, True)])
def test_languoid_query_print_rows_pretty(treedb, as_rows, sort_keys):
    query = (treedb.get_languoids_query(as_rows=as_rows, sort_keys=sort_keys)
             .where(treedb.Languoid.id == 'abin1243'))
    with io.StringIO() as f:
        treedb.print_rows(query, pretty=True, file=f)
        result = f.getvalue()
    assert result
    if as_rows:
        if sort_keys or sys.version_info < (3, 8):
            assert result.startswith("{'__path__': 'abin1243',\n"
                                     " 'languoid': {'altnames': {")
        else:
            assert result.startswith("{'__path__': 'abin1243',\n"
                                     " 'languoid': {'id': 'abin1243',\n"
                                     "              'parent_id': None,\n")
    else:
        if sort_keys or sys.version_info < (3, 8):
            assert result.startswith("{'md.ini': {'__path__': ['abin1243'],\n"
                                     "            'languoid': {'altnames': {")
        else:
            assert result.startswith("{'md.ini': {'__path__': ['abin1243'],\n"
                                     "            'languoid': {'id': 'abin1243',\n"
                                     "                         'parent_id': None,\n")
