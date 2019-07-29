# raw.py - ini content as (path, section, option, line, value) rows

from __future__ import unicode_literals

import io
import csv
import json
import itertools

from ._compat import pathlib
from ._compat import zip, iteritems

from . import _compat

import sqlalchemy as sa

from . import ENCODING

from . import files as _files
from . import backend as _backend
from . import tools as _tools

__all__ = [
    'File', 'Option', 'Value',
    'iterrecords',
    'to_csv', 'to_json', 'to_files',
    'print_stats', 'print_fields',
]

WINDOWSIZE = 500


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
    def is_lines(cls, section, option):
        """Return whether the section option is treated as list of lines."""
        result = cls._fields.get((section, None))
        if result is None:
            # use .get() instead to permit unknown fields as scalar
            return cls._fields[(section, option)]
        return result


class File(_backend.Model):
    """Forward-slash-joined ids from the root to each item."""

    __tablename__ = '_file'

    id = sa.Column(sa.Integer, primary_key=True)
    glottocode = sa.Column(sa.String(8), sa.CheckConstraint('length(glottocode) = 8'), nullable=False, unique=True)
    path = sa.Column(sa.Text, sa.CheckConstraint('length(path) >= 8'), nullable=False, unique=True)
    size = sa.Column(sa.Integer, sa.CheckConstraint('size > 0'), nullable=False)
    sha256 = sa.Column(sa.String(64), sa.CheckConstraint('length(sha256) = 64'), unique=True, nullable=False)

    __table_args__ = (
        sa.CheckConstraint('substr(path, -length(glottocode)) = glottocode'),
    )


class Option(_backend.Model):
    """Unique (section, option) key of the values with lines config."""

    __tablename__ = '_option'

    id = sa.Column(sa.Integer, primary_key=True)
    section = sa.Column(sa.Text, sa.CheckConstraint("section != ''"), nullable=False)
    option = sa.Column(sa.Text, sa.CheckConstraint("option != ''"), nullable=False)
    lines = sa.Column(sa.Boolean, nullable=False)

    __table_args__ = (
        sa.UniqueConstraint(section, option),
    )


class Value(_backend.Model):
    """Item value as (path, section, option, line, value) combination."""

    __tablename__ = '_value'

    file_id = sa.Column(sa.ForeignKey('_file.id'), primary_key=True)
    option_id = sa.Column(sa.ForeignKey('_option.id'), primary_key=True)
    line = sa.Column(sa.Integer, sa.CheckConstraint('line >= 0'), primary_key=True)
    # TODO: consider adding version for selective updates
    value = sa.Column(sa.Text, sa.CheckConstraint("value != ''"), nullable=False)


def _load(root, conn, is_lines=Fields.is_lines):
    insert_file = sa.insert(File, bind=conn).execute
    insert_value = sa.insert(Value, bind=conn).execute

    class Options(dict):
        """Insert optons on demand and cache id and lines config."""

        _insert = sa.insert(Option, bind=conn).execute

        def __missing__(self, key):
            section, option = key
            lines = is_lines(section, option)
            id_, = self._insert(section=section, option=option, lines=lines).inserted_primary_key
            self[key] = result = (id_, lines)
            return result

    options = Options()

    for path_tuple, dentry, cfg in _files.iterconfig(root):
        file_id, = insert_file(glottocode=path_tuple[-1],
                               path='/'.join(path_tuple),
                               size=dentry.stat().st_size,
                               sha256=_tools.sha256sum(dentry.path).hexdigest()).inserted_primary_key
        for section, sec in iteritems(cfg):
            for option, value in iteritems(sec):
                option_id, lines = options[(section, option)]
                if lines:
                    for i, v in enumerate(value.strip().splitlines(), 1):
                        insert_value(file_id=file_id, option_id=option_id,
                                    line=i, value=v)
                else:
                    insert_value(file_id=file_id, option_id=option_id,
                                line=0, value=value)


def iterrecords(bind=_backend.ENGINE, windowsize=WINDOWSIZE):
    """Yield (path, <dict of <dicts of strings/string_lists>>) pairs."""
    select_files = sa.select([File.path], bind=bind).order_by(File.id)
    all_files_values = sa.select([~sa.exists().select_from(File).where(
        ~sa.exists().where(Value.file_id == File.id))], bind=bind)
    # save sa.outerjoin(File, Value) below
    assert all_files_values.scalar(), 'depend on no empty value files'
    select_values = sa.select([
            Value.file_id, Option.section, Option.option, Option.lines, Value.line, Value.value,
        ], bind=bind)\
        .select_from(sa.join(Value, Option))\
        .order_by(Value.file_id, Option.section, Option.option, Value.line)

    groupby = (('file_id',), ('section',), ('option', 'lines'))
    groupby = itertools.starmap(_tools.groupby_attrgetter, groupby)
    groupby_file, groupby_section, groupby_option = groupby

    for in_slice in list(window_slices(File.id, size=windowsize, bind=bind)):
        files = select_files.where(in_slice(File.id)).execute().fetchall()
        if not files:
            continue
        # single thread: no isolation level concerns
        values = select_values.where(in_slice(Value.file_id)).execute().fetchall()
        # join by file_id order index
        for (path,), (_, values) in zip(files, groupby_file(values)):
            record = {
                s: {o: [l.value for l in lines] if islines else next(lines).value
                   for (o, islines), lines in groupby_option(sections)}
                for s, sections in groupby_section(values)}
            yield path, record


def window_slices(key_column, size=WINDOWSIZE, bind=_backend.ENGINE):
    """Yield where clause making function for key_column windows of size."""
    row_num = sa.func.row_number().over(order_by=key_column).label('row_num')
    select_keys = sa.select([key_column.label('key'), row_num]).alias()
    select_keys = sa.select([select_keys.c.key], bind=bind)\
        .where(select_keys.c.row_num % size == 0)

    keys = (k for k, in select_keys.execute())
    try:
        end = next(keys)
    except StopIteration:
        yield lambda c: sa.and_()
        return
    # right-inclusive indexes for windows of given size for continuous keys
    yield lambda c, end=end: (c <= end)
    last = end
    for end in keys:
        yield lambda c, last=last, end=end: sa.and_(c > last, c <= end)
        last = end
    yield lambda c, end=end: (c > end)


def to_csv(filename='raw.csv', bind=_backend.ENGINE, encoding=ENCODING):
    """Write (path, section, option, line, value) rows to <filename>.csv."""
    query = sa.select([
            File.path, Option.section, Option.option, Value.line, Value.value,
        ], bind=bind).select_from(sa.join(File, Value).join(Option))\
        .order_by(File.path, Option.section, Option.option, Value.line)
    rows = query.execute()
    with _compat.csv_open(filename, 'w', encoding=encoding) as f:
        writer = csv.writer(f)
        _compat.csv_write(writer, encoding, header=rows.keys(), rows=rows)


def to_json(filename=None, bind=_backend.ENGINE, encoding=ENCODING):
    """Write (path, json) rows to <databasename>-json.csv."""
    if filename is None:
        filename = '%s-json.csv' % pathlib.Path(bind.url.database).stem
    rows = ((path, json.dumps(data)) for path, data in iterrecords(bind=bind))
    with _compat.csv_open(filename, 'w', encoding=encoding) as f:
        writer = csv.writer(f)
        _compat.csv_write(writer, encoding, header=['path', 'json'], rows=rows)


def to_files(bind=_backend.ENGINE, verbose=False, is_lines=Fields.is_lines):
    """Write (path, section, option, line, value) rows back into config files."""
    def iterpairs(records):
        for p, r in records:
            path_tuple = pathlib.Path(p).parts
            for section, s in iteritems(r):
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield path_tuple, r

    _files.save(iterpairs(iterrecords(bind=bind)), verbose=verbose)


def print_fields(bind=_backend.ENGINE):
    has_scalar = (sa.func.min(Value.line) == 0).label('scalar')
    has_lines = (sa.func.max(Value.line) != 0).label('lines')
    query = sa.select([
            Option.section, Option.option, has_scalar, has_lines,
        ], bind=bind)\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by(Option.section, Option.option)
    print('FIELDS_LIST = {')
    _backend.print_rows(query, '    ({section!r}, {option!r}): {lines},  # 0x{scalar:d}{lines:d}')
    print('}')


def print_stats(bind=_backend.ENGINE, execute=False):
    query = sa.select([
            Option.section, Option.option, sa.func.count().label('n'),
        ], bind=bind)\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by(Option.section, sa.desc('n'))
    _backend.print_rows(query, '{section:<22} {option:<22} {n:,}')


def dropfunc(func, bind=_backend.ENGINE, save=True, verbose=True):
    def wrapper(bind=bind, save=save, verbose=verbose):
        delete_query = func()
        rows_deleted = bind.execute(delete_query).rowcount
        if rows_deleted and save:
            to_files(bind=bind, verbose=verbose)
        return rows_deleted
    return wrapper


@dropfunc
def drop_duplicate_sources():
    other = sa.orm.aliased(Value)
    return sa.delete(Value)\
        .where(sa.exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'sources'))\
        .where(sa.exists()
            .where(other.file_id == Value.file_id)
            .where(other.option_id == Value.option_id)
            .where(other.value == Value.value)
            .where(other.line < Value.line))


@dropfunc
def drop_duplicated_triggers():
    other = sa.orm.aliased(Value)
    return sa.delete(Value)\
        .where(sa.exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'triggers'))\
        .where(sa.exists()
            .where(other.file_id == Value.file_id)
            .where(other.option_id == Value.option_id)
            .where(other.value == Value.value)
            .where(other.line < Value.line))


@dropfunc
def drop_duplicated_crefs():
    other = sa.orm.aliased(Value)
    return sa.delete(Value)\
        .where(sa.exists()
            .where(Option.id == Value.option_id)
            .where(Option.section == 'classification')
            .where(Option.option.in_(('familyrefs', 'subrefs'))))\
        .where(sa.exists()
            .where(other.file_id == Value.file_id)
            .where(other.option_id == Value.option_id)
            .where(other.value == Value.value)
            .where(other.line < Value.line))
