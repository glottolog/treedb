import pytest


pytestmark = pytest.mark.writes


@pytest.mark.skip('missing test isolation')
@pytest.mark.xfail(pytest.CONFIG.option.glottolog_tag == 'master',
                   reason='possibly unnormalized')
@pytest.mark.xfail(pytest.CONFIG.option.glottolog_tag == 'v4.1',
                   reason='float format')
def test_roundtrip(treedb):
    assert treedb.roundtrip() == 0
