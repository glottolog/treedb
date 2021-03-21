from conftest import assert_valid_languoids

import pytest


pytestmark = pytest.FLAGS.skip_exclude_raw


def test_iterlanguoids_from_raw(treedb, n=501):
    items = treedb.iterlanguoids(treedb.engine, from_raw=True)
    assert_valid_languoids(items, n=n)
