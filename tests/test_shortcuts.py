# test_shortcuts.py

import pytest


@pytest.mark.filterwarnings('ignore'
                            ':the imp module is deprecated'
                            ':DeprecationWarning')
def test_pd_read_sql(treedb):
    query = treedb.select([treedb.Languoid])

    df = treedb.pd_read_sql(query, index_col='id')

    assert not df.empty
    df.info()
