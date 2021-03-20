import sqlalchemy as sa

import pytest


@pytest.mark.filterwarnings('ignore'
                            ':the imp module is deprecated'
                            ':DeprecationWarning')
def test_pd_read_sql(treedb):
    query = sa.select(treedb.Languoid)

    df = treedb.pd_read_sql(query, index_col='id')

    if treedb.shortcuts.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        df.info()


def test_pd_read_json_lines(treedb):
    try:
        import pandas
    except ImportError:
        pass
    else:
        assert pandas
        pytest.skip('TODO: check memory usage')

    df = treedb.shortcuts.pd_read_json_lines()

    if treedb.shortcuts.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        df.info()
