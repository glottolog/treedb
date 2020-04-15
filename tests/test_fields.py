# test_fields.py

import re

import pytest

import treedb


@pytest.mark.parametrize('section, option, expected', [('core', 'name', True),
                                                       ('core', 'spam', False),
                                                       ('spam', 'core', False)])
def test_is_known(section, option, expected):
    assert treedb.fields.is_known(section, option) == expected


@pytest.mark.parametrize('section, option, kwargs, expected', [
    ('core', 'name', {}, False),
    ('core', 'links', {}, True),
    ('core', 'WARNS_SCALAR', {}, (None, UserWarning, r'unknown')),
    ('core', 'RAISES_KEYERROR', {'unknown_as_scalar': False}, (KeyError, r'.+')),
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


@pytest.mark.parametrize('section, options, expected', [
    ('core', ['eggs', 'iso639-3', 'name'], ['name', 'iso639-3', 'eggs']),
])
def test_sorted_options(section, options, expected):
    assert treedb.fields.sorted_options(section, options) == expected
