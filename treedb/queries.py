# queries.py - sqlalchemy queries for sqlite3 db

from __future__ import unicode_literals
from __future__ import print_function

from . import _compat

from ._compat import ENCODING

import sqlalchemy as sa
import sqlalchemy.orm

from . import ENGINE

from . import tools as _tools

from .models import (LEVEL,
                     ALTNAME_PROVIDER, IDENTIFIER_SITE,
                     Languoid,
                     languoid_macroarea, languoid_country,
                     Altname, Identifier, Trigger,
                     ClassificationComment, ClassificationRef,
                     Country, Link, Source,
                     Endangerment, EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['print_rows', 'write_csv', 'get_query', 'iterdescendants']


def print_rows(query=None, format_=None, verbose=False, bind=ENGINE):
    if query is None:
        query = get_query()

    if verbose:
        print(query)

    rows = bind.execute(query)

    if format_ is not None:
        rows = _compat.map(format_.format_map, rows)

    for r in rows:
        print(r)


def write_csv(query=None, filename=None, encoding=ENCODING,
              verbose=False, bind=ENGINE):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()

    if filename is None:
        filename = bind.file_with_suffix('.csv').parts[-1]

    if verbose:
        print(query)

    rows = bind.execute(query)
    header = rows.keys()
    return _tools.write_csv(filename, rows, header, encoding)


def get_query(bind=ENGINE):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
    def get_cols(model, label='%s', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = '_%s' % ignore
            cols = [c for c in cols if c.name != ignore and not c.name.endswith(ignore_suffix)]
        return [c.label(label % c.name) for c in cols]

    altnames = [(p, sa.orm.aliased(Altname)) for p in sorted(ALTNAME_PROVIDER)]

    idents = [(s, sa.orm.aliased(Identifier)) for s in sorted(IDENTIFIER_SITE)]

    froms = Languoid.__table__
    for s, i in idents:
        froms = froms.outerjoin(i, sa.and_(i.languoid_id == Languoid.id, i.site == s))

    ltrig, itrig = (sa.orm.aliased(Trigger) for _ in range(2))

    subc, famc = (sa.orm.aliased(ClassificationComment) for _ in range(2))

    subr, famr = (sa.orm.aliased(ClassificationRef) for _ in range(2))

    path, family, language = Languoid.path_family_language()

    select_languoid = sa.select([
            path.label('path'),
            family.label('family_id'),
            language.label('dialect_language_id'),
            Languoid,
            sa.select([sa.func.group_concat(languoid_macroarea.c.macroarea_name, ', ')])
                .where(languoid_macroarea.c.languoid_id == Languoid.id)
                .order_by(languoid_macroarea)
                .label('macroareas'),
            sa.select([sa.func.group_concat(Country.id, ', ')])
                .select_from(languoid_country.join(Country))
                .where(languoid_country.c.languoid_id == Languoid.id)
                .order_by(Country.id)
                .label('countries'),
            sa.select([sa.func.group_concat(Link.printf(), ', ')])
                .where(Link.languoid_id == Languoid.id)
                .order_by(Link.ord)
                .label('links'),
            sa.select([sa.func.group_concat(Source.printf(), ', ')])
                .where(Source.languoid_id == Languoid.id)
                .where(Source.provider == 'glottolog')
                .order_by(Source.ord)
                .label('sources_glottolog'),
            ] + [sa.select([sa.func.group_concat(a.printf(), ', ')])
                    .where(a.languoid_id == Languoid.id)
                    .where(a.provider == p)
                    .order_by(a.ord)
                    .label('altnames_%s' % p)
                 for p, a in altnames]
            + [
            sa.select([sa.func.group_concat(ltrig.trigger, ', ')])
                .where(ltrig.languoid_id == Languoid.id)
                .where(ltrig.field == 'lgcode')
                .order_by(ltrig.ord)
                .label('triggers_lgcode'),
            sa.select([sa.func.group_concat(itrig.trigger, ', ')])
                .where(itrig.languoid_id == Languoid.id)
                .where(itrig.field == 'inlg')
                .order_by(itrig.ord)
                .label('trigggers_inlg'),
            ] + [i.identifier.label('identifier_%s' % s) for s, i in idents]
            + [
            subc.comment.label('classification_sub'),
            sa.select([sa.func.group_concat(subr.printf(), ', ')])
            .where(subr.languoid_id == Languoid.id)
                .where(subr.kind == 'sub')
                .order_by(subr.ord)
                .label('classification_subrefs'),
            famc.comment.label('classification_family'),
            sa.select([sa.func.group_concat(famr.printf(), ', ')])
                .where(famr.languoid_id == Languoid.id)
                .where(famr.kind == 'family')
                .order_by(famr.ord)
                .label('classification_familyrefs'),
            ] + get_cols(Endangerment, label='endangerment_%s')
            + get_cols(EthnologueComment, label='elcomment_%s')
            + get_cols(IsoRetirement, label='iso_retirement_%s')
            + [
            sa.select([sa.func.group_concat(IsoRetirementChangeTo.code, ', ')])
            .where(IsoRetirementChangeTo.languoid_id == Languoid.id)
            .order_by(IsoRetirementChangeTo.ord)
            .label('iso_retirement_change_to'),
        ], bind=bind).select_from(froms
            .outerjoin(subc, sa.and_(subc.languoid_id == Languoid.id, subc.kind == 'sub'))
            .outerjoin(famc, sa.and_(famc.languoid_id == Languoid.id, famc.kind == 'family'))
            .outerjoin(Endangerment)
            .outerjoin(EthnologueComment)
            .outerjoin(IsoRetirement))\
        .order_by(Languoid.id)

    return select_languoid


def iterdescendants(parent_level=None, child_level=None, bind=ENGINE):
    """Yield pairs of (parent id, sorted list of their descendant ids)."""
    # TODO: implement ancestors/descendants as sa.orm.relationship()
    # see https://bitbucket.org/zzzeek/sqlalchemy/issues/4165
    Parent, Child = (sa.orm.aliased(Languoid, name=n) for n in ('parent', 'child'))
    tree = Languoid.tree()

    select_pairs = sa.select([
            Parent.id.label('parent_id'), Child.id.label('child_id'),
        ], bind=bind).select_from(
            sa.outerjoin(Parent, tree, tree.c.parent_id == Parent.id)
            .outerjoin(Child, tree.c.child_id == Child.id))\
        .order_by('parent_id', 'child_id')

    if parent_level is not None:
        if parent_level == 'top':
            cond = (Parent.parent_id == sa.null())
        elif parent_level in LEVEL:
            cond = (Parent.level == parent_level)
        else:
            raise ValueError('invalid parent_level: %r' % parent_level)
        select_pairs = select_pairs.where(cond)

    if child_level is not None:
        if child_level not in LEVEL:
            raise ValueError('invalid child_level: %r' % child_level)
        select_pairs = select_pairs.where(Child.level == child_level)

    rows = select_pairs.execute()

    for parent_id, grp in _tools.groupby_attrgetter('parent_id')(rows):
        _, c = next(grp)
        if c is None:
            descendants = []
        else:
            descendants = [c] + [c for _, c in grp]
        yield parent_id, descendants
