"""``pytest`` command-line options and fixtures."""

import os
import string

import pytest

DEFAULT_GLOTTOLOG_TAG = 'v5.2.1'

RUN_WRITES = '--run-writes'

SKIP_SLOW = '--skip-slow'

SKIP_PANDAS = '--skip-pandas'

SKIP_SQLPARSE = '--skip-sqlparse'

GLOTTOLOG_TAG = '--glottolog-tag'

EXCLUDE_RAW = '--exclude-raw'


def pytest_addoption(parser):
    parser.addoption(RUN_WRITES, action='store_true',
                     help='run tests with pytest.mark.writes')

    parser.addoption(SKIP_SLOW, action='store_true',
                     help='skip tests with pytest.mark.slow')

    parser.addoption(SKIP_PANDAS, action='store_true',
                     help='skip tests with optional pandas dependency')

    parser.addoption(SKIP_SQLPARSE, action='store_true',
                     help='skip tests with optional sqlparse dependency')

    parser.addoption('--file-engine', action='store_true',
                     help='use configured file engine instead of in-memory db')

    parser.addoption('--file-engine-tag', metavar='TMPL', default='-$glottolog_tag',
                     help='filename extra tag template rendered with pytestconfig.option')

    parser.addoption(GLOTTOLOG_TAG, metavar='GIT_TAG', default=DEFAULT_GLOTTOLOG_TAG,
                     help='tag or branch to clone/checkout from Glottolog master repo')

    parser.addoption('--glottolog-repo-root', metavar='PATH',
                     help='pass root=PATH to treedb.configure()')

    parser.addoption('--rebuild', action='store_true',
                     help='pass rebuild=True to treedb.load()')

    parser.addoption('--force-rebuild', action='store_true',
                     help='pass force_rebuild=True to treedb.load()')

    parser.addoption(EXCLUDE_RAW, action='store_true',
                     help='pass exclude_raw=True to treedb.load()')

    parser.addoption('--loglevel-debug', action='store_true',
                     help='pass loglevel=DEBUG to treedb.configure()')

    parser.addoption('--log-sql', action='store_true',
                     help='pass log_sql=True to treedb.configure()')

    parser.addoption('--no-sqlalchemy-warn-20', action='store_false',
                     dest='sqlalchemy_warn_20',
                     help="don't set os.environ['SQLALCHEMY_WARN_20']")


def pytest_configure(config):
    file_engine_tag = string.Template(config.option.file_engine_tag)
    file_engine_tag = file_engine_tag.substitute(config.option.__dict__)
    config.option.file_engine_tag = file_engine_tag

    config.addinivalue_line('markers', f'writes: skip unless {RUN_WRITES} is given')
    config.addinivalue_line('markers', f'slow: skip if {SKIP_SLOW} flag is given')
    config.addinivalue_line('markers', f'pandas: skip if {SKIP_PANDAS} flag is given')
    config.addinivalue_line('markers', f'sqlparse: skip if {SKIP_SQLPARSE} flag is given')

    config.addinivalue_line('markers', f'skipif_glottolog_tag: skip for specific {GLOTTOLOG_TAG}')
    config.addinivalue_line('markers', f'xfail_glottolog_tag: xfail for given {GLOTTOLOG_TAG}')

    config.addinivalue_line('markers', f'raw: skip if {EXCLUDE_RAW} flag is given')

    if config.option.sqlalchemy_warn_20:
        os.environ['SQLALCHEMY_WARN_20'] = 'true'


def pytest_collection_modifyitems(config, items):
    def itermarkers():
        for keyword, option in {'writes': RUN_WRITES,
                                'slow': SKIP_SLOW,
                                'pandas': SKIP_PANDAS,
                                'sqlparse': SKIP_SQLPARSE,
                                'raw': EXCLUDE_RAW}.items():
            skip = not option.startswith('--run-')
            if config.getoption(option) == skip:
                reason = 'require' if not skip else 'skipped by'
                marker = pytest.mark.skip(reason=f'{reason} {option} flag')
                yield keyword, marker

    keyword_markers = dict(itermarkers())

    for item in items:
        for keyword, marker in keyword_markers.items():
            if keyword in item.keywords:
                item.add_marker(marker)
        for option in ('skipif_glottolog_tag', 'xfail_glottolog_tag'):
            for marker in item.iter_markers(option):
                if config.getoption(GLOTTOLOG_TAG) in marker.args:
                    marker_cls = {'skipif': pytest.mark.skip,
                                  'xfail': pytest.mark.xfail}[option.partition('_')[0]]
                    item.add_marker(marker_cls(**marker.kwargs))


def get_configure_kwargs(pytestconfig, *, title: str):
    memory_tag = '' if pytestconfig.option.file_engine else '-memory'
    title = f'{title}{memory_tag}{pytestconfig.option.file_engine_tag}'
    kwargs = {'title': title,
              'title_memory_tag': '',
              'engine': f'{title}.sqlite3'
                        if pytestconfig.option.file_engine else None,
              'loglevel': 'DEBUG' if pytestconfig.option.loglevel_debug else 'WARNING'}

    if pytestconfig.option.glottolog_repo_root is not None:
        kwargs['root'] = pytestconfig.option.glottolog_repo_root

    if pytestconfig.option.log_sql:
        kwargs['log_sql'] = True

    return kwargs


@pytest.fixture(scope='session')
def bare_treedb(pytestconfig):
    import treedb as bare_treedb

    kwargs = get_configure_kwargs(pytestconfig, title=f'{bare_treedb.__title__}-bare')
    bare_treedb.configure(**kwargs)

    bare_treedb.checkout_or_clone(pytestconfig.option.glottolog_tag)

    return bare_treedb


@pytest.fixture(scope='session')
def empty_treedb(pytestconfig, bare_treedb):
    empty_treedb = bare_treedb

    kwargs = get_configure_kwargs(pytestconfig, title=f'{empty_treedb.__title__}-empty')
    empty_treedb.configure(**kwargs)

    empty_treedb.load(_only_create_tables=True,
                      rebuild=pytestconfig.option.rebuild,
                      force_rebuild=pytestconfig.option.force_rebuild,
                      exclude_raw=pytestconfig.option.exclude_raw)

    return empty_treedb


@pytest.fixture(scope='session')
def treedb(pytestconfig, bare_treedb):
    treedb = bare_treedb

    kwargs = get_configure_kwargs(pytestconfig, title=treedb.__title__)
    treedb.configure(**kwargs)

    treedb.load(rebuild=pytestconfig.option.rebuild,
                force_rebuild=pytestconfig.option.force_rebuild,
                exclude_raw=pytestconfig.option.exclude_raw)

    return treedb


@pytest.fixture(scope='session')
def treedb_raw(treedb):
    import treedb.raw

    return treedb
