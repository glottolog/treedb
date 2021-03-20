import pytest

BOOK = 'book1242'

RAMO = 'ramo1244'

TREE = {BOOK: [('book1242', 'book1242', 0, True)],
        RAMO: [('ramo1244', 'ramo1244', 0, False),
               ('ramo1244', 'kand1307', 1, False),
               ('ramo1244', 'stge1234', 2, False),
               ('ramo1244', 'newi1242', 3, False),
               ('ramo1244', 'meso1253', 4, False),
               ('ramo1244', 'west2818', 5, False),
               ('ramo1244', 'ocea1241', 6, False),
               ('ramo1244', 'east2712', 7, False),
               ('ramo1244', 'cent2237', 8, False),
               ('ramo1244', 'mala1545', 9, False),
               ('ramo1244', 'aust1307', 10, True)]}

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
