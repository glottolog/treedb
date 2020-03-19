# queries.py - sqlalchemy queries for sqlite3 db

import hashlib
import logging

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import aliased

from . import tools as _tools

from . import ENGINE

from .models import (LEVEL, ALTNAME_PROVIDER, IDENTIFIER_SITE,
                     Languoid,
                     languoid_macroarea,
                     languoid_country, Country,
                     Link, Source, Altname, Trigger, Identifier,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['print_rows', 'write_csv', 'hash_csv',
           'get_query',
           'iterdescendants']


log = logging.getLogger(__name__)


def print_rows(query=None, format_=None, verbose=False, bind=ENGINE):
    if query is None:
        query = get_query()

    if verbose:
        print(query)

    rows = bind.execute(query)

    if format_ is not None:
        rows = map(format_.format_map, rows)

    for r in rows:
        print(r)


def write_csv(query=None, filename=None,
              dialect=_tools.DIALECT, encoding=_tools.ENCODING,
              verbose=False, bind=ENGINE):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()

    if filename is None:
        filename = bind.file_with_suffix('.csv').name
    filename = _tools.path_from_filename(filename).expanduser()

    log.info('write csv: %r', filename)
    if verbose:
        print(query)

    rows = bind.execute(query)
    header = rows.keys()
    log.info('header: %r', header)

    return _tools.write_csv(filename, rows, header=header,
                            dialect=dialect, encoding=encoding)


def hash_csv(query=None, raw=False, name=None,
             dialect=_tools.DIALECT, encoding=_tools.ENCODING,
             bind=ENGINE):
    if query is None:
        query = get_query()

    name = name if name is not None else 'sha256'
    log.info('hash csv: %r', name)

    rows = bind.execute(query)
    header = rows.keys()
    log.info('header: %r', header)

    result = hashlib.new(name)
    assert hasattr(result, 'hexdigest')
    _tools.write_csv(result, rows, header=header,
                     dialect=dialect, encoding=encoding)

    if not raw:
        result = result.hexdigest()
    return result


def get_query(bind=ENGINE):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
    def get_cols(model, label='%s', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = f'_{ignore}'
            cols = [c for c in cols if c.name != ignore and not c.name.endswith(ignore_suffix)]
        return [c.label(label % c.name) for c in cols]

    altnames = [(p, aliased(Altname)) for p in sorted(ALTNAME_PROVIDER)]

    idents = [(s, aliased(Identifier)) for s in sorted(IDENTIFIER_SITE)]

    froms = Languoid.__table__
    for s, i in idents:
        froms = froms.outerjoin(i, sa.and_(i.languoid_id == Languoid.id, i.site == s))

    ltrig, itrig = (aliased(Trigger) for _ in range(2))

    subc, famc = (aliased(ClassificationComment) for _ in range(2))

    subr, famr = (aliased(ClassificationRef) for _ in range(2))

    path, family, language = Languoid.path_family_language()

    group_concat = sa.func.group_concat

    select_languoid = select([
            Languoid.id,
            Languoid.name,
            Languoid.level,
            Languoid.parent_id,
            path.label('path'),
            family.label('family_id'),
            language.label('dialect_language_id'),
            Languoid.hid,
            Languoid.iso639_3,
            Languoid.latitude,
            Languoid.longitude,
            select([group_concat(languoid_macroarea.c.macroarea_name, ', ')])
                .where(languoid_macroarea.c.languoid_id == Languoid.id)
                .order_by(languoid_macroarea)
                .label('macroareas'),
            select([group_concat(Country.id, ', ')])
                .select_from(languoid_country.join(Country))
                .where(languoid_country.c.languoid_id == Languoid.id)
                .order_by(Country.id)
                .label('countries'),
            select([group_concat(Link.printf(), ', ')])
                .where(Link.languoid_id == Languoid.id)
                .order_by(Link.ord)
                .label('links'),
            select([group_concat(Source.printf(), ', ')])
                .where(Source.languoid_id == Languoid.id)
                .where(Source.provider == 'glottolog')
                .order_by(Source.ord)
                .label('sources_glottolog'),
            ] + [select([group_concat(a.printf(), ', ')])
                    .where(a.languoid_id == Languoid.id)
                    .where(a.provider == p)
                    .order_by(a.ord)
                    .label('altnames_%s' % p)
                 for p, a in altnames]
            + [
            select([group_concat(ltrig.trigger, ', ')])
                .where(ltrig.languoid_id == Languoid.id)
                .where(ltrig.field == 'lgcode')
                .order_by(ltrig.ord)
                .label('triggers_lgcode'),
            select([group_concat(itrig.trigger, ', ')])
                .where(itrig.languoid_id == Languoid.id)
                .where(itrig.field == 'inlg')
                .order_by(itrig.ord)
                .label('trigggers_inlg'),
            ] + [i.identifier.label('identifier_%s' % s) for s, i in idents]
            + [
            subc.comment.label('classification_sub'),
            select([group_concat(subr.printf(), ', ')])
            .where(subr.languoid_id == Languoid.id)
                .where(subr.kind == 'sub')
                .order_by(subr.ord)
                .label('classification_subrefs'),
            famc.comment.label('classification_family'),
            select([group_concat(famr.printf(), ', ')])
                .where(famr.languoid_id == Languoid.id)
                .where(famr.kind == 'family')
                .order_by(famr.ord)
                .label('classification_familyrefs'),
            ] + get_cols(Endangerment, label='endangerment_%s')
            + [EndangermentSource.printf().label('endangerment_source')]
            + get_cols(EthnologueComment, label='elcomment_%s')
            + get_cols(IsoRetirement, label='iso_retirement_%s')
            + [
            select([group_concat(IsoRetirementChangeTo.code, ', ')])
            .where(IsoRetirementChangeTo.languoid_id == Languoid.id)
            .order_by(IsoRetirementChangeTo.ord)
            .label('iso_retirement_change_to'),
        ], bind=bind).select_from(froms
            .outerjoin(subc, sa.and_(subc.languoid_id == Languoid.id, subc.kind == 'sub'))
            .outerjoin(famc, sa.and_(famc.languoid_id == Languoid.id, famc.kind == 'family'))
            .outerjoin(sa.join(Endangerment, EndangermentSource))
            .outerjoin(EthnologueComment)
            .outerjoin(IsoRetirement))\
        .order_by(Languoid.id)

    return select_languoid


def iterdescendants(parent_level=None, child_level=None, bind=ENGINE):
    """Yield pairs of (parent id, sorted list of their descendant ids)."""
    # TODO: implement ancestors/descendants as sa.orm.relationship()
    # see https://bitbucket.org/zzzeek/sqlalchemy/issues/4165
    Parent, Child = (aliased(Languoid, name=n) for n in ('parent', 'child'))
    tree = Languoid.tree()

    select_pairs = select([
            Parent.id.label('parent_id'), Child.id.label('child_id'),
        ], bind=bind).select_from(
            sa.outerjoin(Parent, tree, tree.c.parent_id == Parent.id)
            .outerjoin(Child, tree.c.child_id == Child.id))\
        .order_by('parent_id', 'child_id')

    if parent_level is not None:
        if parent_level == 'top':
            cond = (Parent.parent_id == None)
        elif parent_level in LEVEL:
            cond = (Parent.level == parent_level)
        else:
            raise ValueError(f'invalid parent_level: {parent_level!r}')
        select_pairs.append_whereclause(cond)

    if child_level is not None:
        if child_level not in LEVEL:
            raise ValueError(f'invalid child_level: {child_level!r}')
        select_pairs.append_whereclause(Child.level == child_level)

    rows = select_pairs.execute()

    for parent_id, grp in _tools.groupby_attrgetter('parent_id')(rows):
        _, c = next(grp)
        if c is None:
            descendants = []
        else:
            descendants = [c] + [c for _, c in grp]
        yield parent_id, descendants
