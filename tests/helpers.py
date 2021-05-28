import itertools

MB = 2**20


def pairwise(iterable):
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def get_assert_head(items, *, n):
    head = list(itertools.islice(items, n))

    assert head
    assert len(head) == n

    return head


def assert_nonempty_string(obj):
    assert obj is not None
    assert isinstance(obj, str)


def assert_nonempty_string_tuple(obj):
    assert obj is not None
    assert isinstance(obj, tuple)
    assert all(isinstance(o, str) for o in obj)
    assert obj
    assert all(obj)


def assert_nonempty_dict(obj):
    assert obj is not None
    assert isinstance(obj, dict)
    assert obj


def assert_file_size_between(path, min, max, *, unit=MB):
    assert path is not None
    assert path.exists()
    assert path.is_file()
    assert min * unit <= path.stat().st_size <= max * unit


def assert_valid_languoids(items, *, n):
    for path, languoid in get_assert_head(items, n=n):
        assert_nonempty_string_tuple(path)
        assert_nonempty_dict(languoid)

        for key in ('id', 'level', 'name'):
            assert_nonempty_string(languoid[key])

        assert languoid['parent_id'] or languoid['parent_id'] is None

        assert languoid['level'] in ('family', 'language', 'dialect')
