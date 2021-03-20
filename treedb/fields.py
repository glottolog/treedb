# fields.py - known md.ini (section, option) pairs

"""Define known (section, option) pairs and if they are lists of lines."""

import itertools
import logging
import warnings

from . import _tools

__all__ = ['is_lines',
           'sorted_sections',
           'sorted_options']

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

SECTION_ORDER = [s for s, _ in itertools.groupby(FIELDS, lambda x: x[0])]
SECTION_ORDER = _tools.Ordering.fromlist(SECTION_ORDER)

FIELD_ORDER = _tools.Ordering.fromlist(FIELDS)

CORE_SECTIONS = frozenset({CORE})

KEEP_EMPTY_SECTIONS = CORE_SECTIONS | frozenset({SOURCES})

KEEP_EMPTY_OPTIONS = frozenset({(SOURCES, 'glottolog'),
                                (ISO_RETIREMENT, 'change_to')})


log = logging.getLogger(__name__)


def is_known(section, option):
    """Retun True if the section option is known or in an ALL_OPTIONS section."""
    return (section, ALL_OPTIONS) in FIELDS or (section, option) in FIELDS


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
