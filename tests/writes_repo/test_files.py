import pytest

from treedb import glottolog


pytestmark = pytest.mark.writes


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
def test_roundtrip_dry_run(treedb):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError

    files_written = treedb.files.roundtrip(dry_run=True)

    assert files_written == 0
    assert glottolog.git_status_is_clean(treedb.root)


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
@pytest.mark.xfail_glottolog_tag('v4.1', reason='float normalization: https://github.com/glottolog/glottolog/pull/495',
                                 raises=AssertionError)
def test_roundtrip(treedb):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError

    files_written = treedb.files.roundtrip()

    assert files_written == 0
    assert glottolog.git_status_is_clean(treedb.root)
