from helpers import assert_valid_languoids

import pytest


pytestmark = pytest.mark.raw


def test_iterlanguoids_from_raw(treedb, n=501):
    items = treedb.iterlanguoids('raw')
    assert_valid_languoids(items, n=n)
