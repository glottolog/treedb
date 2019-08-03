# models.py

from __future__ import unicode_literals

import warnings

from sqlalchemy import (Column, Integer, String, Text, Boolean,
                        ForeignKey, CheckConstraint, UniqueConstraint)

from ..backend import Model

__all__ = [
    'File', 'Option', 'Value',
    'Fields',
]


class File(Model):
    """Forward-slash-joined ids from the root to each item."""

    __tablename__ = '_file'

    id = Column(Integer, primary_key=True)

    glottocode = Column(String(8), CheckConstraint('length(glottocode) = 8'), nullable=False, unique=True)

    path = Column(Text, CheckConstraint('length(path) >= 8'), nullable=False, unique=True)

    size = Column(Integer, CheckConstraint('size > 0'), nullable=False)
    sha256 = Column(String(64), CheckConstraint('length(sha256) = 64'), unique=True, nullable=False)

    __table_args__ = (
        CheckConstraint('substr(path, -length(glottocode)) = glottocode'),
    )


class Option(Model):
    """Unique (section, option) key of the values with lines config."""

    __tablename__ = '_option'

    id = Column(Integer, primary_key=True)

    section = Column(Text, CheckConstraint("section != ''"), nullable=False)
    option = Column(Text, CheckConstraint("option != ''"), nullable=False)

    lines = Column(Boolean)

    __table_args__ = (
        UniqueConstraint(section, option),
    )


class Value(Model):
    """Item value as (path, section, option, line, value) combination."""

    __tablename__ = '_value'

    file_id = Column(ForeignKey('_file.id'), primary_key=True)
    line = Column(Integer, CheckConstraint('line >= 0'), primary_key=True)
    option_id = Column(ForeignKey('_option.id'), primary_key=True)

    # TODO: consider adding version for selective updates
    value = Column(Text, CheckConstraint("value != ''"), nullable=False)

    __table_args__ = (
        UniqueConstraint(file_id, line),
    )


class Fields(object):
    """Define known (section, option) pairs and if they are lists of lines."""

    _fields = {
        ('core', 'name'): False,
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

        ('sources', None): True,

        ('altnames', None): True,

        ('triggers', None): True,

        ('identifier', None): False,

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
        ('iso_retirement', 'comment'): False,
    }

    @classmethod
    def is_known(cls, section, option):
        return (section, None) in cls._fields or (section, option) in cls._fields

    @classmethod
    def is_lines(cls, section, option, unknown_as_scalar=True):
        """Return whether the section option is treated as list of lines."""
        result = cls._fields.get((section, None))
        if result is None:
            try:
                return cls._fields[section, option]
            except KeyError:
                msg = 'section %r unknown option %r' % (section, option)
                warnings.warn(msg)
                if unknown_as_scalar:
                    return None
                raise
        return result
