# raw.py - ini content as (path, section, option, line, value) rows

from __future__ import unicode_literals

import warnings
import itertools

from ._compat import ENCODING, zip, iteritems

import sqlalchemy as sa

from . import ENGINE, ROOT

from . import files as _files
from . import queries as _queries
from . import tools as _tools

from .backend import Model


__all__ = [
    'File', 'Option', 'Value',
    'iterrecords',
    'to_raw_csv', 'to_files',
    'print_stats',
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
    def is_lines(cls, section, option, unknown_as_scalar=True):
        """Return whether the section option is treated as list of lines."""
        result = cls._fields.get((section, None))
        if result is None:
            try:
                return cls._fields[(section, option)]
            except KeyError:
                msg = 'section %r unknown option %r' % (section, option),
                warnings.warn(msg)
                if unknown_as_scalar:
                    return None
                raise
        return result


class File(Model):
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


class Option(Model):
    """Unique (section, option) key of the values with lines config."""

    __tablename__ = '_option'

    id = sa.Column(sa.Integer, primary_key=True)

    section = sa.Column(sa.Text, sa.CheckConstraint("section != ''"), nullable=False)
    option = sa.Column(sa.Text, sa.CheckConstraint("option != ''"), nullable=False)

    lines = sa.Column(sa.Boolean)

    __table_args__ = (
        sa.UniqueConstraint(section, option),
    )


class Value(Model):
    """Item value as (path, section, option, line, value) combination."""

    __tablename__ = '_value'

    file_id = sa.Column(sa.ForeignKey('_file.id'), primary_key=True)
    line = sa.Column(sa.Integer, sa.CheckConstraint('line >= 0'), primary_key=True)
    option_id = sa.Column(sa.ForeignKey('_option.id'), primary_key=True)

    # TODO: consider adding version for selective updates
    value = sa.Column(sa.Text, sa.CheckConstraint("value != ''"), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint(file_id, line),
    )


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

    def _itervalues(cfg, file_id, options=Options()):
        get_line = _tools.next_count()
        for section, sec in iteritems(cfg):
            for option, value in iteritems(sec):
                option_id, lines = options[(section, option)]
                if lines:
                    for v in value.strip().splitlines():
                        yield {'file_id': file_id, 'option_id': option_id,
                               'line': get_line(), 'value': v}
                else:
                    yield {'file_id': file_id, 'option_id': option_id,
                           'line': get_line(), 'value': value}

    for path_tuple, dentry, cfg in _files.iterfiles(root):
        file_params = {
            'glottocode': path_tuple[-1],
            'path': '/'.join(path_tuple),
            'size': dentry.stat().st_size,
            'sha256': _tools.sha256sum(dentry.path).hexdigest(),
        }
        file_id, = insert_file(file_params).inserted_primary_key
        value_params = list(_itervalues(cfg, file_id))
        insert_value(value_params)


def iterrecords(bind=ENGINE, windowsize=WINDOWSIZE, skip_unknown=True):
    """Yield (<path_part>, ...), <dict of <dicts of strings/string_lists>>) pairs."""
    select_files = sa.select([File.path], bind=bind).order_by(File.id)
    # depend on no empty value files (save sa.outerjoin(File, Value) below)
    select_values = sa.select([
            Value.file_id, Option.section, Option.option, Option.lines, Value.value,
        ], bind=bind)\
        .select_from(sa.join(Value, Option))\
        .order_by(Value.file_id, Option.section, Value.line, Option.option)
    if skip_unknown:
        select_values.append_whereclause(Option.lines != None)

    groupby = (('file_id',), ('section',), ('option', 'lines'))
    groupby = itertools.starmap(_tools.groupby_attrgetter, groupby)
    groupby_file, groupby_section, groupby_option = groupby

    for in_slice in window_slices(File.id, size=windowsize, bind=bind):
        files = select_files.where(in_slice(File.id)).execute().fetchall()
        # single thread: no isolation level concerns
        values = select_values.where(in_slice(Value.file_id)).execute().fetchall()
        # join by file_id total order index
        for (path,), (_, values) in zip(files, groupby_file(values)):
            record = {
                s: {o: [l.value for l in lines] if islines else next(lines).value
                   for (o, islines), lines in groupby_option(sections)}
                for s, sections in groupby_section(values)}
            yield tuple(path.split('/')), record


def window_slices(key_column, size=WINDOWSIZE, bind=ENGINE):
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


def to_raw_csv(filename=None, encoding=ENCODING, bind=ENGINE):
    """Write (path, section, option, line, value) rows to filename."""
    if filename is None:
        filename = bind.file_with_suffix('.raw.csv').parts[-1]

    select_values = sa.select([
            File.path, Option.section, Option.option, Value.line, Value.value,
        ]).select_from(sa.join(File, Value).join(Option))\
        .order_by(File.path, Option.section, Option.option, Value.line)

    return _queries.write_csv(select_values, filename, encoding, bind=bind)


def to_files(root=ROOT, bind=ENGINE, verbose=True, is_lines=Fields.is_lines):
    """Write (path, section, option, line, value) rows back into config files."""
    records = iterrecords(bind)

    def iterpairs(records):
        for p, r in records:
            for section, s in iteritems(r):
                for option in s:
                    if is_lines(section, option):
                        s[option] = '\n'.join([''] + s[option])
            yield p, r

    return _files.save(iterpairs(records), root, verbose=verbose)


def print_stats(bind=ENGINE):
    select_nvalues = sa.select([
            Option.section, Option.option, sa.func.count().label('n'),
        ], bind=bind)\
        .select_from(sa.join(Option, Value))\
        .group_by(Option.section, Option.option)\
        .order_by(Option.section, sa.desc('n'))

    _queries.print_rows(select_nvalues,
                        format_='{section:<22} {option:<22} {n:,}', bind=bind)


def dropfunc(func, save=True, verbose=True, bind=ENGINE):
    def wrapper(save=save, verbose=verbose, bind=bind):
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
