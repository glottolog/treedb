import sqlalchemy as sa

import pytest

INFO_KWARGS = {'memory_usage': 'deep'}


pytestmark = pytest.mark.pandas


@pytest.mark.filterwarnings('ignore'
                            ':The MetaData.bind argument is deprecated'
                            ':DeprecationWarning')
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
        df.info(**INFO_KWARGS)


def test_pd_read_json_lines(treedb):
    query = sa.select(sa.func.json_object('id', treedb.Languoid.id,
                                          'name', treedb.Languoid.name))
    df = treedb.pd_read_json_lines(query, concat_ignore_index=True)

    if treedb.backend.pandas.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        assert list(df.columns) == ['id', 'name']
        assert df.index.is_unique
        df.info(**INFO_KWARGS)


@pytest.mark.xfail(reason="broken pd.read_json(orient='index', lines=True)",
                   raises=AttributeError)
def test_pd_read_json_lines_orient_index(treedb):
    languoid = sa.func.json_object('name', treedb.Languoid.name,
                                   'level', treedb.Languoid.level)
    languoid = sa.func.json_object(treedb.Languoid.id, languoid)
    query = sa.select(languoid)

    df = treedb.pd_read_json_lines(query, orient='index')

    if treedb.backend.pandas.PANDAS is None:
        assert df is None
    else:
        assert not df.empty
        assert df.index.name == 'id'
        assert list(df.columns) == ['name', 'level']
        assert df.index.is_unique
        df.info(**INFO_KWARGS)
