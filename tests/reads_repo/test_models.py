import re

import pytest
import sqlalchemy as sa

import treedb.models as _models

BOOK = 'book1242'

RAMO = 'ramo1244'

TREE = {BOOK: [(BOOK, BOOK, 0, True)],
        RAMO: [(RAMO, RAMO, 0, False),
               (RAMO, 'kand1307', 1, False),
               (RAMO, 'stge1234', 2, False),
               (RAMO, 'newi1242', 3, False),
               (RAMO, 'meso1253', 4, False),
               (RAMO, 'west2818', 5, False),
               (RAMO, 'ocea1241', 6, False),
               (RAMO, 'east2712', 7, False),
               (RAMO, 'cent2237', 8, False),
               (RAMO, 'mala1545', 9, False),
               (RAMO, 'aust1307', 10, True)]}

FULL = {'include_self': True, 'with_steps': True, 'with_terminal': True}

EXCLUSIVE = {'with_steps': True, 'with_terminal': True}


@pytest.mark.parametrize('child_id, parent_id, kwargs, expected', [
    (BOOK, None, FULL, TREE[BOOK]),
    (BOOK, None, EXCLUSIVE, []),
    (BOOK, None, {}, []),
    (RAMO, None, FULL, TREE[RAMO]),
    (RAMO, None, EXCLUSIVE, TREE[RAMO][1:]),
    (RAMO, None, {}, [(c, p) for c, p, _, _ in TREE[RAMO][1:]]),
])
def test_languoid_tree(treedb, child_id, parent_id, kwargs, expected):
    tree = treedb.Languoid.tree(**kwargs)

    select_tree = tree.select()

    if child_id is not None:
        select_tree = select_tree.where(tree.c.child_id == child_id)

    if parent_id is not None:
        select_tree = select_tree.where(tree.c.parent_id == parent_id)

    with treedb.connect() as conn:
        result = conn.execute(select_tree).all()

    assert result == expected


@pytest.mark.parametrize('model, whereclause, expected_repr', [
    (_models.Languoid, _models.Languoid.id == '3adt1234',
     r"<Languoid id='3adt1234' level='dialect' name='3Ad-Tekles'>"),
    (_models.Macroarea, _models.Macroarea.name == 'Eurasia',
     r"<Macroarea 'Eurasia'>"),
    (_models.Country, _models.Country.id == 'RU',
     r"<Country id='RU' name='Russian Federation'>"),
    (_models.Bibfile, _models.Bibfile.name == 'hh',
     r"<Bibfile id=\d+ name='hh'>"),
    (_models.AltnameProvider, _models.AltnameProvider.name == 'multitree',
     "<AltnameProvider name='multitree'>"),
    (_models.EndangermentSource, _models.EndangermentSource.name == 'E23',
     "<EndangermentSource id=\d+ name='E23' bibitem_id=None pages=None>"),
    (_models.IdentifierSite, _models.IdentifierSite.name == 'multitree',
     "<IdentifierSite name='multitree'>")])
def test_repr(treedb, model, whereclause, expected_repr):
    query = sa.select(model)
    if whereclause is not None:
        query = query.where(whereclause)

    with treedb.Session() as session:
        inst = session.execute(query).scalars().first()

    assert re.match(expected_repr, inst)
