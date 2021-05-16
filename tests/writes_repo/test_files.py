import pytest

from treedb import glottolog


pytestmark = pytest.mark.writes


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized',
                                 raises=AssertionError)
@pytest.mark.parametrize('kwargs', [
    pytest.param({'dry_run': True}, id='dry_run'),
    # needs to be run at the end (no cleanup on fail)
    pytest.param({'dry_run': False}, id='default'),  
])
def test_roundtrip(treedb, kwargs):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError

    files_written = treedb.files.roundtrip(**kwargs)

    clean = glottolog.git_status_is_clean(treedb.root)
    if not clean:
        print(glottolog.git_status(treedb.root))

    assert files_written == 0
    assert clean
