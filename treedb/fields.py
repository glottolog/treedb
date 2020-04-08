# fields.py - known md.ini (section, option) pairs

"""Define known (section, option) pairs and if they are lists of lines."""

import itertools
import logging
import warnings

from . import tools as _tools

__all__ = ['is_lines',
           'sorted_sections',
           'sorted_options']

ALL_OPTIONS = object()

FIELDS = {('core', 'name'): False,
          ('core', 'hid'): False,
          ('core', 'level'): False,
          ('core', 'iso639-3'): False,
          ('core', 'latitude'): False,
          ('core', 'longitude'): False,
          ('core', 'macroareas'): True,
          ('core', 'countries'): True,
          ('core', 'name_comment'): False,
          # FIXME: core hapaxes
          ('core', 'comment'): False,
          ('core', 'location'): False,
          ('core', 'name_pronunciation'): False,
          ('core', 'speakers'): False,

          ('core', 'links'): True,

          ('sources', ALL_OPTIONS): True,

          ('altnames', ALL_OPTIONS): True,

          ('triggers', ALL_OPTIONS): True,

          ('identifier', ALL_OPTIONS): False,

          ('classification', 'sub'): False,
          ('classification', 'subrefs'): True,
          ('classification', 'family'): False,
          ('classification', 'familyrefs'): True,

          ('endangerment', 'status'): False,
          ('endangerment', 'source'): False,
          ('endangerment', 'date'): False,
          ('endangerment', 'comment'): False,

          ('hh_ethnologue_comment', 'isohid'): False,
          ('hh_ethnologue_comment', 'comment_type'): False,
          ('hh_ethnologue_comment', 'ethnologue_versions'): False,
          ('hh_ethnologue_comment', 'comment'): False,

          ('iso_retirement', 'code'): False,
          ('iso_retirement', 'name'): False,
          ('iso_retirement', 'change_request'): False,
          ('iso_retirement', 'effective'): False,
          ('iso_retirement', 'reason'): False,
          ('iso_retirement', 'change_to'): True,
          ('iso_retirement', 'remedy'): False,
          ('iso_retirement', 'comment'): False}

SECTION_ORDER = [s for s, _ in itertools.groupby(FIELDS, lambda x: x[0])]
SECTION_ORDER = _tools.Ordering.fromlist(SECTION_ORDER)

FIELD_ORDER = _tools.Ordering.fromlist(FIELDS)


log = logging.getLogger(__name__)


def is_known(section, option):
    return (section, ALL_OPTIONS) in FIELDS or (section, option) in FIELDS


def is_lines(section, option, *, unknown_as_scalar=True):
    """Return whether the section option is treated as list of lines."""
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
    fields = FIELD_ORDER.sorted((section, o) for o in options)
    return [o for _, o in fields]
