# queries.py - sqlalchemy queries for sqlite3 db

import hashlib
import logging
import warnings

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import aliased

import csv23

from . import tools as _tools

from . import ENGINE

from .models import (LEVEL, ALTNAME_PROVIDER, IDENTIFIER_SITE,
                     Languoid,
                     languoid_macroarea,
                     languoid_country, Country,
                     Link, Source, Bibfile, Bibitem,
                     Altname, Trigger, Identifier,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['print_rows', 'write_csv', 'hash_csv',
           'get_query', 'get_json_query',
           'iterdescendants']


log = logging.getLogger(__name__)


def print_rows(query=None, *, format_=None, verbose=False, bind=ENGINE):
    if query is None:
        query = get_query()

    if verbose:
        print(query)

    rows = bind.execute(query)

    if format_ is not None:
        rows = map(format_.format_map, rows)

    for r in rows:
        print(r)


def write_csv(query=None, filename=None, *, verbose=False,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING, bind=ENGINE):
    """Write get_query() example query (or given query) to CSV, return filename."""
    if query is None:
        query = get_query()

    if filename is None:
        filename = bind.file_with_suffix('.csv').name
    filename = _tools.path_from_filename(filename)

    log.info('write csv: %r', filename)
    path = _tools.path_from_filename(filename)
    if path.exists():
        warnings.warn(f'delete present file: {path!r}')
        path.unlink()

    if verbose:
        print(query)

    rows = bind.execute(query)
    header = rows.keys()
    log.info('header: %r', header)

    return csv23.write_csv(filename, rows, header=header,
                           dialect=dialect, encoding=encoding)


def hash_csv(query=None, *,
             raw=False, name=None,
             dialect=csv23.DIALECT, encoding=csv23.ENCODING, bind=ENGINE):
    if query is None:
        query = get_query()

    name = name if name is not None else 'sha256'
    log.info('hash csv: %r', name)

    rows = bind.execute(query)
    header = rows.keys()
    log.info('header: %r', header)

    result = hashlib.new(name)
    assert hasattr(result, 'hexdigest')
    csv23.write_csv(result, rows, header=header,
                     dialect=dialect, encoding=encoding)

    if not raw:
        result = result.hexdigest()
    return result


def get_query(*, bind=ENGINE, separator=', ', ordered='id'):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
    group_concat = lambda x: sa.func.group_concat(x, separator)

    path, family, language = Languoid.path_family_language()

    macroareas = select([group_concat(languoid_macroarea.c.macroarea_name)])\
        .where(languoid_macroarea.c.languoid_id == Languoid.id)\
        .order_by(languoid_macroarea)\
        .label('macroareas')

    countries = select([group_concat(Country.id)])\
        .select_from(languoid_country.join(Country))\
        .where(languoid_country.c.languoid_id == Languoid.id)\
        .order_by(Country.id)\
        .label('countries')

    links = select([group_concat(Link.printf())])\
        .where(Link.languoid_id == Languoid.id)\
        .order_by(Link.ord)\
        .label('links')

    s_bibfile, s_bibitem = map(aliased, (Bibfile, Bibitem))

    sources_glottolog = select([Source.printf(s_bibfile, s_bibitem)])\
        .where(Source.provider == 'glottolog')\
        .where(Source.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .where(Source.bibitem_id == s_bibitem.id)\
        .where(s_bibitem.bibfile_id == s_bibfile.id)\
        .order_by(s_bibfile.name, s_bibitem.bibkey)

    sources_glottolog = select([group_concat(sources_glottolog.c.printf)])\
        .label('sources_glottolog')

    altnames = [select([group_concat(a.printf())])
                    .where(a.provider == p)
                    .where(a.languoid_id == Languoid.id)
                    .order_by(a.name, a.lang)
                    .label(f'altnames_{p}')
                for p, a in {p: aliased(Altname)
                             for p in sorted(ALTNAME_PROVIDER)}.items()]

    ltrig, itrig = (aliased(Trigger) for _ in range(2))

    triggers_lgcode = select([group_concat(ltrig.trigger)])\
        .where(ltrig.field == 'lgcode')\
        .where(ltrig.languoid_id == Languoid.id)\
        .order_by(ltrig.ord)\
        .label('triggers_lgcode')

    trigggers_inlg = select([group_concat(itrig.trigger)])\
        .where(itrig.field == 'inlg')\
        .where(itrig.languoid_id == Languoid.id)\
        .order_by(itrig.ord)\
        .label('trigggers_inlg')

    idents = {s: aliased(Identifier) for s in sorted(IDENTIFIER_SITE)}

    froms = Languoid.__table__
    for s, i in idents.items():
        froms = froms.outerjoin(i, sa.and_(i.site == s,
                                           i.languoid_id == Languoid.id))

    idents = [i.identifier.label(f'identifier_{s}') for s, i in idents.items()]

    subr, csr_bibfile, csr_bibitem = map(aliased,
                                         (ClassificationRef, Bibfile, Bibitem))

    classification_subrefs = select([subr.printf(csr_bibfile, csr_bibitem)])\
        .where(subr.kind == 'sub')\
        .where(subr.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .where(subr.bibitem_id == csr_bibitem.id)\
        .where(csr_bibitem.bibfile_id == csr_bibfile.id)\
        .order_by(subr.ord)

    classification_subrefs = select([group_concat(classification_subrefs.c.printf)])\
        .label('classification_subrefs')

    famr, cfr_bibfile, cfr_bibitem = map(aliased,
                                         (ClassificationRef, Bibfile, Bibitem))

    classification_familyrefs = select([famr.printf(cfr_bibfile, cfr_bibitem)])\
        .where(famr.kind == 'family')\
        .where(famr.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .where(famr.bibitem_id == cfr_bibitem.id)\
        .where(cfr_bibitem.bibfile_id == cfr_bibfile.id)\
        .order_by(famr.ord)

    classification_familyrefs = select([group_concat(classification_familyrefs.c.printf)])\
        .label('classification_familyrefs')

    e_bibfile, e_bibitem = map(aliased, (Bibfile, Bibitem))

    endangerment_source = EndangermentSource.printf(e_bibfile, e_bibitem)\
        .label('endangerment_source')

    iso_retirement_change_to = select([group_concat(IsoRetirementChangeTo.code)])\
        .where(IsoRetirementChangeTo.languoid_id == Languoid.id)\
        .order_by(IsoRetirementChangeTo.ord)\
        .label('iso_retirement_change_to')

    def get_cols(model, label='{name}', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = f'_{ignore}'
            cols = [c for c in cols if c.name != ignore
                    and not c.name.endswith(ignore_suffix)]
        return [c.label(label.format(name=col.c.name)) for c in cols]

    subc, famc = (aliased(ClassificationComment) for _ in range(2))

    classifcation_sub = subc.comment.label('classification_sub')
    classification_family = famc.comment.label('classification_family')

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
            macroareas,
            countries,
            links,
            sources_glottolog,
            ] + altnames + [
            triggers_lgcode,
            trigggers_inlg,
            ] + idents + [
            classifcation_sub,
            classification_subrefs,
            classification_family,
            classification_familyrefs,
            ] + get_cols(Endangerment, label='endangerment_{name}') + [
            endangerment_source,
            ] + get_cols(EthnologueComment, label='elcomment_{name}')
            + get_cols(IsoRetirement, label='iso_retirement_{name}') + [
            iso_retirement_change_to,
        ], bind=bind)\
        .select_from(froms
            .outerjoin(subc, sa.and_(subc.languoid_id == Languoid.id, subc.kind == 'sub'))
            .outerjoin(famc, sa.and_(famc.languoid_id == Languoid.id, famc.kind == 'family'))
            .outerjoin(sa.join(Endangerment, EndangermentSource)
                       .outerjoin(sa.join(e_bibitem, e_bibfile)))
            .outerjoin(EthnologueComment)
            .outerjoin(IsoRetirement))

    _apply_ordered(select_languoid, path, ordered=ordered)

    return select_languoid


def _apply_ordered(select_languoid, path, *, ordered):
    if ordered is False:
        pass
    elif ordered in (True, 'id'):
        select_languoid.append_order_by(Languoid.id)
    elif ordered == 'path':
       select_languoid.append_order_by(path)
    else:
        raise ValueError(f'ordered={ordered!r} not implemented')


def get_json_query(*, bind=ENGINE, ordered='id'):
    json_array = sa.func.json_array
    json_object = sa.func.json_object
    group_array = sa.func.json_group_array
    group_object = sa.func.json_group_object

    macroareas = select([group_array(languoid_macroarea.c.macroarea_name)])\
        .where(languoid_macroarea.c.languoid_id == Languoid.id)\
        .order_by(languoid_macroarea)

    countries = select([group_array(Country.jsonf())])\
        .select_from(languoid_country.join(Country))\
        .where(languoid_country.c.languoid_id == Languoid.id)\
        .order_by(Country.id)\
        .as_scalar()

    links = select([group_array(Link.jsonf())])\
        .where(Link.languoid_id == Languoid.id)\
        .order_by(Link.ord)\
        .as_scalar()

    s_bibfile, s_bibitem = map(aliased, (Bibfile, Bibitem))

    sources = select([
            Source.provider,
            Source.jsonf(s_bibfile, s_bibitem),
        ]).where(Source.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .where(Source.bibitem_id == s_bibitem.id)\
        .where(s_bibitem.bibfile_id == s_bibfile.id)\
        .order_by(Source.provider, s_bibfile.name, s_bibitem.bibkey)

    sources = select([json_object(sources.c.provider,
                                  group_array(sa.func.json(sources.c.jsonf)))])\
        .group_by(sources.c.provider)\
        .as_scalar()

    altnames = select([
            Altname.provider,
            Altname.jsonf(),
        ]).where(Altname.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .order_by(Altname.provider, Altname.name, Altname.lang)

    altnames = select([json_object(altnames.c.provider,
                                   group_array(sa.func.json(altnames.c.jsonf)))])\
        .group_by(altnames.c.provider)\
        .as_scalar()

    triggers = select([
            Trigger.field,
            Trigger.trigger,
        ]).where(Trigger.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .order_by(Trigger.field, Trigger.ord)

    triggers = select([json_object(triggers.c.field,
                                   group_array(triggers.c.trigger))])\
        .group_by(triggers.c.field)\
        .as_scalar()

    identifier = select([group_object(Identifier.site,
                                      Identifier.identifier)])\
        .where(Identifier.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    classification_comment = select([
            ClassificationComment.kind.label('key'),
            ClassificationComment.comment.label('value'),
        ]).where(ClassificationComment.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    cr_bibfile, cr_bibitem = map(aliased, (Bibfile, Bibitem))

    classification_refs = select([
            (ClassificationRef.kind + 'refs').label('key'),
            ClassificationRef.jsonf(cr_bibfile, cr_bibitem),
        ]).where(ClassificationRef.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .where(ClassificationRef.bibitem_id == cr_bibitem.id)\
        .where(cr_bibitem.bibfile_id == cr_bibfile.id)

    classification_refs = select([
            classification_refs.c.key,
            group_array(classification_refs.c.jsonf).label('value'),
        ]).group_by(classification_refs.c.key)

    classification = classification_comment.union_all(classification_refs)

    classification = select([json_object(classification.c.key,
                                         classification.c.value)])\
        .select_from(classification)\
        .as_scalar()

    e_bibfile, e_bibitem = map(aliased, (Bibfile, Bibitem))

    endangerment = select([Endangerment.jsonf(EndangermentSource,
                                              e_bibfile, e_bibitem)])\
        .select_from(sa.join(Endangerment, EndangermentSource)
                     .outerjoin(sa.join(e_bibitem, e_bibfile)))\
        .where(Endangerment.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    hh_ethnologue_comment = select([EthnologueComment.jsonf()])\
        .where(EthnologueComment.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    iso_retirement = select([IsoRetirement.jsonf()])\
        .where(IsoRetirement.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    path = Languoid.path_json()

    select_languoid = select([json_array(path, json_object(
            'id', Languoid.id,
            'parent_id', Languoid.parent_id,
            'level', Languoid.level,
            'name', Languoid.name,
            'hid', Languoid.hid,
            'iso639_3', Languoid.iso639_3,
            'latitude', Languoid.latitude,
            'longitude', Languoid.longitude,
            'macroareas', macroareas,
            'countries', countries,
            'links', links,
            'sources', sources,
            'altnames', altnames,
            'triggers', triggers,
            'identifier', identifier,
            'classification', classification,
            'endangerment', endangerment,
            'hh_ethnologue_comment', hh_ethnologue_comment,
            'iso_retirement', iso_retirement,
        ))], bind=bind)\
        .select_from(Languoid)

    _apply_ordered(select_languoid, path, ordered=ordered)

    return select_languoid


def iterdescendants(parent_level=None, child_level=None, *, bind=ENGINE):
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
