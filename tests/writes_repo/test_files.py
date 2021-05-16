import pytest


pytestmark = pytest.mark.writes


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
@pytest.mark.parametrize('kwargs', [
    pytest.param({}, id='replace=default'),
    pytest.param({'replace': True}, id='replace=True',
                 marks=[pytest.mark.skip('TODO: test_roundtrip() isolation'),
                        pytest.mark.xfail_glottolog_tag('v4.1',
                                                        reason='float format: https://github.com/glottolog/glottolog/pull/495',
                                                        raises=AssertionError)]),
])
def test_roundtrip(treedb, kwargs):
    assert treedb.files.roundtrip(**kwargs) == 0
