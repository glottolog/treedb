import collections
import functools

import pytest

ZeroDict = functools.partial(collections.defaultdict, int)

FILES_WRITTEN = {'raw_lines': ZeroDict(),
                 'raw': ZeroDict(),
                 'tables': ZeroDict({# replace 'Country Name (ID)' with 'ID'
                                     'v4.3-treedb-fixes': 8_528,
                                     'v4.2.1': 8_528,
                                     'v4.2': None,
                                     'v4.1': 8_540})}


pytestmark = pytest.mark.writes


@pytest.mark.parametrize('source',
                         [pytest.param('raw_lines', marks=pytest.mark.raw),
                          pytest.param('raw', marks=pytest.mark.raw),
                          'tables'],
                         ids=lambda x: f'source={x}')
def test_write_files(pytestconfig, treedb, source):
    expected = FILES_WRITTEN[source].get(pytestconfig.option.glottolog_tag)

    files_written = treedb.write_files(source=source,
                                       dry_run=True,
                                       require_nwritten=expected,
                                       bind=treedb.engine)
    if expected is not None:
        assert files_written == expected
    else:
        assert 0 <= files_written <= 40_000
