"""Define known ``md.ini`` (section, option) pairs and if they are lists of lines."""

import logging
import typing
import warnings

from . import _globals
from . import _tools

__all__ = ['is_lines',
           'sorted_sections',
           'sorted_options',
           'parse_lines', 'format_lines',
           'join_lines_inplace']

SECTIONS = ('core',
            'sources',
            'altnames', 'triggers', 'identifier',
            'classification',
            'endangerment', 'hh_ethnologue_comment', 'iso_retirement')

(CORE,
 SOURCES,
 ALTNAMES, TRIGGERS, IDENTIFIER,
 CLASSIFICATION,
 ENGANGERMENT, HH_ETHNOLOGUE_COMMENT, ISO_RETIREMENT) = SECTIONS

ALL_OPTIONS = object()

FIELDS = {(CORE, 'name'): False,
          (CORE, 'hid'): False,
          (CORE, 'level'): False,
          (CORE, 'iso639-3'): False,
          (CORE, 'latitude'): False,
          (CORE, 'longitude'): False,

          (CORE, 'macroareas'): True,
          (CORE, 'countries'): True,

          (CORE, 'name_comment'): False,

          # FIXME: core hapaxes
          (CORE, 'comment'): False,
          (CORE, 'location'): False,
          (CORE, 'name_pronunciation'): False,
          (CORE, 'speakers'): False,

          (CORE, 'links'): True,
          (CORE, 'timespan'): False,

          (SOURCES, ALL_OPTIONS): True,

          (ALTNAMES, ALL_OPTIONS): True,

          (TRIGGERS, ALL_OPTIONS): True,

          (IDENTIFIER, ALL_OPTIONS): False,

          (CLASSIFICATION, 'sub'): False,
          (CLASSIFICATION, 'subrefs'): True,
          (CLASSIFICATION, 'family'): False,
          (CLASSIFICATION, 'familyrefs'): True,

          (ENGANGERMENT, 'status'): False,
          (ENGANGERMENT, 'source'): False,
          (ENGANGERMENT, 'date'): False,
          (ENGANGERMENT, 'comment'): False,

          (HH_ETHNOLOGUE_COMMENT, 'isohid'): False,
          (HH_ETHNOLOGUE_COMMENT, 'comment_type'): False,
          (HH_ETHNOLOGUE_COMMENT, 'ethnologue_versions'): False,
          (HH_ETHNOLOGUE_COMMENT, 'comment'): False,

          (ISO_RETIREMENT, 'code'): False,
          (ISO_RETIREMENT, 'name'): False,
          (ISO_RETIREMENT, 'change_request'): False,
          (ISO_RETIREMENT, 'effective'): False,
          (ISO_RETIREMENT, 'reason'): False,
          (ISO_RETIREMENT, 'change_to'): True,
          (ISO_RETIREMENT, 'remedy'): False,
          (ISO_RETIREMENT, 'comment'): False}

SECTION_ORDER = [s for s, _ in _tools.groupby_itemgetter(0)(FIELDS)]
SECTION_ORDER = _tools.Ordering.fromlist(SECTION_ORDER, start_index=1)

FIELD_ORDER = _tools.Ordering.fromlist(FIELDS, start_index=1)

CORE_SECTIONS = frozenset({CORE})

# avoid writing empty 'core', 'timespan' options to files (first added in Glottolog 4.2, treedb 0.11)
OMIT_EMPTY_CORE_OPTIONS = frozenset({'timespan'})

KEEP_EMPTY_SECTIONS = CORE_SECTIONS | frozenset({SOURCES})

KEEP_EMPTY_OPTIONS = frozenset({(CORE, 'countries'),
                                (SOURCES, 'glottolog'),
                                (ISO_RETIREMENT, 'change_to')})


log = logging.getLogger(__name__)


def is_known(section, option):
    """Retun True if the section option is known or in an ALL_OPTIONS section."""
    return (section, ALL_OPTIONS) in FIELDS or (section, option) in FIELDS


def is_all_options(section, option):
    """Retun True if the section option is in an ALL_OPTIONS section."""
    return (section, ALL_OPTIONS) in FIELDS


def is_lines(section, option, *, unknown_as_scalar=True):
    """Return True if the section option is treated as list of lines."""
    result = FIELDS.get((section, ALL_OPTIONS))

    if result is None:
        try:
            return FIELDS[section, option]
        except KeyError:
            warnings.warn(f'section {section!r} unknown option {option!r}')

            if unknown_as_scalar:
                log.warning('treating %r as scalar', (section, option))
                return None

            log.exception('unknown option')
            raise

    return result


sorted_sections = SECTION_ORDER.sorted


def sorted_options(section, options):
    """Return the given section options as sorted list in canonical order."""
    fields = FIELD_ORDER.sorted((section, o) for o in options)
    return [o for _, o in fields]


def parse_lines(value):
    r"""

    >>> parse_lines(None)
    []

    >>> parse_lines(' spam\neggs\n  ')
    ['spam', 'eggs']
    """
    if value is None:
        return []
    return value.strip().splitlines()


def format_lines(value):
    r"""

    >>> format_lines(['spam', 'eggs'])
    '\nspam\neggs'

    >>> format_lines([])
    ''
    """
    lines = [''] + value
    return '\n'.join(lines)


RawRecordType = typing.Mapping[str, typing.Mapping[str, str]]


RawRecordItem = typing.Tuple[typing.Optional[_globals.PathType], RawRecordType]


def join_lines_inplace(record_item: _globals.RecordItem) -> RawRecordItem:
    path, record = record_item
    for name, section in record.items():
        for option in section:
            if is_lines(name, option):
                lines = format_lines(section[option])
                section[option] = lines
    return path, record
