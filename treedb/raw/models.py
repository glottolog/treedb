"""Raw tables schema."""

import sqlalchemy as sa

from sqlalchemy import (Column, Integer, String, Text, Boolean,
                        ForeignKey, CheckConstraint, UniqueConstraint)

from .._globals import REGISTRY as registry

__all__ = ['File', 'Option', 'Value']

PREFIX = '_'


@registry.mapped
class File:
    """Forward-slash-joined ids from the root to each item."""

    __tablename__ = f'{PREFIX}file'

    id = Column(Integer, primary_key=True)

    glottocode = Column(String(8), CheckConstraint('length(glottocode) = 8'),
                        nullable=False, unique=True)

    path = Column(Text, CheckConstraint('length(path) >= 8 AND (length(path) + 1) % 9 = 0'),
                  nullable=False, unique=True)

    size = Column(Integer, CheckConstraint('size > 0'), nullable=False)
    sha256 = Column(String(64), CheckConstraint('length(sha256) = 64'),
                    unique=True, nullable=False)

    __table_args__ = (CheckConstraint('substr(path, -length(glottocode))'
                                      ' = glottocode'),)

    @classmethod
    def path_depth(cls, label='path_depth'):
        return ((sa.func.length(cls.path) + 1) / 9).label(label)


@registry.mapped
class Option:
    """Unique (section, option) key of the values with lines config."""

    __tablename__ = f'{PREFIX}option'

    id = Column(Integer, primary_key=True)

    section = Column(Text, CheckConstraint("section != ''"), nullable=False)
    option = Column(Text, CheckConstraint("option != ''"), nullable=False)

    is_lines = Column(Boolean(create_constraint=True))

    defined = Column(Boolean(create_constraint=True), nullable=False)
    defined_any_options = Column(Boolean(create_constraint=True), nullable=False)

    ord_section = Column(Integer, CheckConstraint('ord_section >= 1'))
    ord_option = Column(Integer, CheckConstraint('ord_section >= 0'))

    __table_args__ = (UniqueConstraint(section, option),
                      CheckConstraint('(is_lines IS NULL) = (defined = 0)'),
                      CheckConstraint('defined = 1 OR defined_any_options = 0'),
                      CheckConstraint('(defined = 0) = (ord_section IS NULL)'),
                      CheckConstraint('ord_section IS NOT NULL'
                                      ' OR ord_option IS NULL'))


@registry.mapped
class Value:
    """Item value as (path, section, option, line, value) combination."""

    __tablename__ = f'{PREFIX}value'

    file_id = Column(ForeignKey('_file.id'), primary_key=True)
    option_id = Column(ForeignKey('_option.id'), primary_key=True)
    line = Column(Integer, CheckConstraint('line > 0'), primary_key=True)

    # TODO: consider adding version for selective updates
    value = Column(Text, CheckConstraint("value != ''"), nullable=False)

    __table_args__ = (UniqueConstraint(file_id, line),
                      {'info': {'without_rowid': True}})
