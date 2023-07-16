import pytest

from treedb import glottolog


pytestmark = pytest.mark.writes


@pytest.mark.xfail_glottolog_tag('master', reason='possibly unnormalized master',
                                 raises=AssertionError)
@pytest.mark.xfail_glottolog_tag('v4.8', reason='extra v4.8 endangerment whitespace',
                                 # https://github.com/glottolog/glottolog/pull/981
                                 raises=AssertionError)
def test_roundtrip(treedb):
    if not glottolog.git_status_is_clean(treedb.root):
        raise RuntimeError(f'root must be clean for test: {treedb.root!r}')

    result = treedb.languoids.files.roundtrip()

    clean = glottolog.git_status_is_clean(treedb.root)
    if not clean:
        print(glottolog.git_status(treedb.root))

    if result is not None:
        raise RuntimeError(f'expected result is not None: {result!r}')
    assert clean
