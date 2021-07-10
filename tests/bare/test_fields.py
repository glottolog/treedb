import re

import pytest

import treedb


@pytest.mark.parametrize('section, option, expected', [
    pytest.param('core', 'name', True,
                 id='section=core, option=name'),
    pytest.param('core', 'spam', False,
                 id='section=core, option=unknown'),
    pytest.param('spam', 'core', False,
                 id='section=unknown, option=unknown'),
])
def test_is_known(section, option, expected):
    assert treedb.languoids.fields.is_known(section, option) == expected


@pytest.mark.parametrize('section, option, kwargs, expected', [
    pytest.param('core', 'name', {}, False,
                 id='section=core, option=name'),
    pytest.param('core', 'links', {}, True,
                 id='section=core, option=links'),
    pytest.param('core', 'WARNS_SCALAR', {}, (None, UserWarning, r'unknown'),
                 id='section=core, option=unknown'),
    pytest.param('core', 'RAISES_KEYERROR', {'unknown_as_scalar': False}, (KeyError, r'.+'),
                 id='section=core, option=unknown, strict'),
])
def test_is_lines(recwarn, section, option, kwargs, expected):
    if isinstance(expected, tuple):
        if len(expected) == 3:
            expected, warning, match = expected

            assert treedb.languoids.fields.is_lines(section, option, **kwargs) == expected

            w = recwarn.pop(warning)
            assert re.search(match, str(w.message))
        else:
            exception, match = expected

            with pytest.raises(exception, match=match):
                treedb.languoids.fields.is_lines(section, option, **kwargs)

            recwarn.pop(UserWarning)
    else:
        assert treedb.languoids.fields.is_lines(section, option, **kwargs) == expected

    assert not recwarn


@pytest.mark.parametrize('section, options, expected', [
    pytest.param('core',
                 ['eggs', 'iso639-3', 'name'],
                 ['name', 'iso639-3', 'eggs'],
                 id='section=core, options=eggs-iso639-3-name'),
])
def test_sorted_options(section, options, expected):
    assert treedb.languoids.fields.sorted_options(section, options) == expected
