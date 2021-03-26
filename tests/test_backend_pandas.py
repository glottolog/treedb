import sqlalchemy as sa

import pytest


@pytest.mark.filterwarnings('ignore'
                            ':the imp module is deprecated'
                            ':DeprecationWarning')
def test_pd_read_sql(treedb):
    query = sa.select(treedb.Languoid)

    df = treedb.pd_read_sql(query, index_col='id')

    if treedb.backend.pandas.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        assert df.index.name == 'id'
        assert list(df.columns)[:5] == ['name', 'level', 'parent_id',
                                        'hid', 'iso639_3']
        assert df.index.is_unique
        df.info()


def test_pd_read_json_lines(treedb):
    df = treedb.pd_read_languoids()

    if treedb.backend.pandas.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        assert df.index.name == 'id'
        assert list(df.columns) == ['path', 'languoid']
        assert df.index.is_unique
        df.info()
