import pytest

from treedb import glottolog


pytestmark = pytest.mark.writes


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
def test_roundtrip(treedb):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError

    files_written = treedb.files.roundtrip()

    assert files_written == 0
    assert glottolog.git_status_is_clean(treedb.root)


@pytest.mark.skip('TODO: test_roundtrip_replace() isolation')
@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
@pytest.mark.xfail_glottolog_tag('v4.1', reason='float format: https://github.com/glottolog/glottolog/pull/495',
                                 raises=AssertionError)
def test_roundtrip_replace(treedb):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError

    treedb.files.roundtrip(replace=True)

    assert glottolog.git_status_is_clean(treedb.root)
