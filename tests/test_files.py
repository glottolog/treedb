# test_files.py

import pytest


@pytest.mark.skip('TODO: improve isolation')
@pytest.mark.xfail(pytest.FLAGS.glottolog_tag == 'master',
                   reason='possibly unnormalized')
@pytest.mark.xfail(pytest.FLAGS.glottolog_tag == 'v4.1',
                   reason='float format')
def test_roundtrip(treedb):
    assert treedb.roundtrip() == 0
