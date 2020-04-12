# test_fields.py

import re

import pytest

import treedb


@pytest.mark.parametrize('section, option, expected', [
    ('core', 'name', True),
    ('core', 'spam', False),
    ('spam', 'core', False),
])
def test_is_known(section, option, expected):
    assert treedb.fields.is_known(section, option) == expected


@pytest.mark.parametrize('section, option, kwargs, expected', [
    ('core', 'name', {}, False),
    ('core', 'links',{}, True),
    ('core', 'WARNS_SCALAR', {}, (None, UserWarning, r'unknown')),
    ('core', 'RAISES_UNKNOWN', {'unknown_as_scalar': False}, (KeyError, r'.+')),
])
def test_is_lines(recwarn, section, option, kwargs, expected):
    if isinstance(expected, tuple):
        if len(expected) == 3:
            expected, warning, match = expected

            assert treedb.fields.is_lines(section, option, **kwargs) == expected

            w = recwarn.pop(warning)
            assert re.search(match, str(w.message))
        else:
            exception, match = expected

            with pytest.raises(exception, match=match):
                treedb.fields.is_lines(section, option, **kwargs)

            recwarn.pop(UserWarning)
    else:
        assert treedb.fields.is_lines(section, option, **kwargs) == expected

    assert not recwarn


def test_sorted_options(section='core', options=('eggs', 'iso639-3', 'name')):
    result = treedb.fields.sorted_options(section, options)
    assert result == ['name', 'iso639-3', 'eggs']
