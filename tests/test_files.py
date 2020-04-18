# test_files.py

import pytest


@pytest.mark.skip('TODO: improve isolation')
@pytest.mark.xfail(pytest.FLAGS.glottolog_tag == 'master',
                   reason='possibly unnormalized')
@pytest.mark.xfail(pytest.FLAGS.glottolog_tag == 'v4.1',
                   reason='float format')
def test_write_files(treedb):
    assert treedb.write_files() == 0
