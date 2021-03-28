# pytest command-line flags and fixtures

import os
import itertools
import types

import pytest

GLOTTOLOG_TAG = 'v4.3-treedb-fixes'

MB = 2**20


os.environ['SQLALCHEMY_WARN_20'] = 'true'


def pytest_addoption(parser):
    parser.addoption('--skip-slow', action='store_true',
                     help='skip tests that are marked as slow')

    parser.addoption('--file-engine', action='store_true',
                     help='use configured file engine instead of in-memory db')

    parser.addoption('--glottolog-tag', default=GLOTTOLOG_TAG,
                     help='tag or branch to clone from Glottolog master repo')

    parser.addoption('--glottolog-repo-root', metavar='PATH',
                     help='pass root=PATH to treedb.configure()')

    parser.addoption('--rebuild', action='store_true',
                     help='pass rebuild=True to treedb.load()')

    parser.addoption('--force-rebuild', action='store_true',
                     help='pass force_rebuild=True to treedb.load()')

    parser.addoption('--exclude-raw', dest='exclude_raw', action='store_true',
                     help='pass exlcude_raw=True to treedb.load()')

    parser.addoption('--loglevel-debug', action='store_true',
                     help='pass loglevel=DEBUG to treedb.configure()')

    parser.addoption('--log-sql', action='store_true',
                     help='pass log_sql=True to treedb.configure()')


def pytest_configure(config):
    options = ('file_engine', 'glottolog_tag', 'glottolog_repo_root',
               'rebuild', 'force_rebuild', 'exclude_raw',
               'loglevel_debug', 'log_sql')

    FLAGS = types.SimpleNamespace(**{o: config.getoption(o) for o in options})

    FLAGS.skip_exclude_raw = pytest.mark.skipif(FLAGS.exclude_raw,
                                                reason='skipped by'
                                                       '--exclude-raw')

    pytest.FLAGS = FLAGS

    pytest.skip_slow = pytest.mark.skipif(config.getoption('--skip-slow'),
                                          reason='skipped by --skip-slow flag')


def get_configure_kwargs(*, title: str, memory_engine=None):
    kwargs = {'title': title}

    if not pytest.FLAGS.file_engine:
        kwargs['engine'] = memory_engine

    if pytest.FLAGS.glottolog_repo_root is not None:
        kwargs['root'] = pytest.FLAGS.glottolog_repo_root

    if pytest.FLAGS.loglevel_debug:
        kwargs['loglevel'] = 'DEBUG'

    if pytest.FLAGS.log_sql:
        kwargs['log_sql'] = True

    return kwargs


@pytest.fixture(scope='session')
def bare_treedb():
    import treedb as bare_treedb

    kwargs = get_configure_kwargs(title='{bare_treedb.__title__}-bare')
    bare_treedb.configure(**kwargs)

    bare_treedb.checkout_or_clone(pytest.FLAGS.glottolog_tag)

    return bare_treedb


@pytest.fixture(scope='session')
def empty_treedb(bare_treedb):
    empty_treedb = bare_treedb

    kwargs = get_configure_kwargs(title=f'{empty_treedb.__title__}-empty')
    empty_treedb.configure(**kwargs)

    empty_treedb.load(_only_create_tables=True,
                      rebuild=pytest.FLAGS.rebuild,
                      force_rebuild=pytest.FLAGS.force_rebuild,
                      exclude_raw=pytest.FLAGS.exclude_raw)

    return empty_treedb


@pytest.fixture(scope='session')
def treedb(bare_treedb):
    treedb = bare_treedb

    kwargs = get_configure_kwargs(title=treedb.__title__)
    treedb.configure(**kwargs)

    treedb.load(rebuild=pytest.FLAGS.rebuild,
                force_rebuild=pytest.FLAGS.force_rebuild,
                exclude_raw=pytest.FLAGS.exclude_raw)

    return treedb


@pytest.fixture(scope='session')
def treedb_raw(treedb):
    import treedb.raw

    return treedb


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
