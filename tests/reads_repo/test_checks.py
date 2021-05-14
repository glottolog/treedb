import pytest


def test_check(treedb):
    assert treedb.check()


@pytest.mark.skip('TODO: improve output on failiure')
@pytest.mark.raw
def test_compare_languoids(treedb):
    assert treedb.compare_languoids()
