from conftest import (get_assert_head,
                      assert_nonempty_string,
                      assert_nonempty_string_tuple,
                      assert_nonempty_dict,
                      assert_valid_languoids)


def test_parse(bare_treedb, *, n=100):
    from treedb import files as _files

    records = _files.iterrecords()

    items = bare_treedb.records.pipe(records, dump=False, convert_lines=True)

    assert_valid_languoids(items, n=n)


def test_dump(bare_treedb, *, n=100):
    languoids = bare_treedb.iterlanguoids()

    items = bare_treedb.records.pipe(languoids, dump=True, convert_lines=False)

    for path, record in get_assert_head(items, n=n):
        assert_nonempty_string_tuple(path)
        assert_nonempty_dict(record)

        assert_nonempty_string(record['core']['name'])

        assert record['core']['level'] in ('family', 'language', 'dialect')
