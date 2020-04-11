# queries.py - batteries-included sqlalchemy queries for sqlite3 db

import hashlib
import logging
import itertools
import warnings

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import aliased

import csv23

from . import (tools as _tools,
               views as _views)

from . import ENGINE

from .models import (LEVEL, FAMILY, LANGUAGE, DIALECT,
                     SPECIAL_FAMILIES, BOOKKEEPING,
                     ALTNAME_PROVIDER, IDENTIFIER_SITE,
                     Languoid,
                     languoid_macroarea,
                     languoid_country, Country,
                     Link, Source, Bibfile, Bibitem,
                     Altname, Trigger, Identifier,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['print_rows', 'write_csv', 'hash_csv', 'hash_rows',
           'get_query',
           'write_json_query_csv', 'get_json_query',
           'print_languoid_stats', 'get_stats_query',
           'iterdescendants']


log = logging.getLogger(__name__)


def print_rows(query=None, *, format_=None, verbose=False, bind=ENGINE):
    if query is None:
        query = get_query(bind=bind)

    if not isinstance(query, sa.sql.base.Executable):
        rows = iter(query)
    else:
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
        query = get_query(bind=bind)

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
        query = get_query(bind=bind)

    rows = bind.execute(query)
    header = rows.keys()

    return hash_rows(rows, header=header, name=name, raw=raw,
                     dialect=dialect, encoding=encoding)


def hash_rows(rows, *, header=None, name=None, raw=False,
              dialect=csv23.DIALECT, encoding=csv23.ENCODING):
    if name is None:
        name = 'sha256'

    log.info('hash rows with %r, header: %r', name, header)
    result = hashlib.new(name)
    assert hasattr(result, 'hexdigest')

    csv23.write_csv(result, rows, header=header,
                    dialect=dialect, encoding=encoding)

    if not raw:
        result = result.hexdigest()
    return result


@_views.register_view('example')
def get_query(*, ordered='id', separator=', ', bind=ENGINE):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
    group_concat = lambda x: sa.func.group_concat(x, separator)

    path, family, language = Languoid.path_family_language()

    macroareas = select([group_concat(languoid_macroarea.c.macroarea_name)
                         .label('macroareas')])\
                 .where(languoid_macroarea.c.languoid_id == Languoid.id)\
                 .order_by(languoid_macroarea)\
                 .label('macroareas')

    countries = select([group_concat(Country.id).label('countries')])\
                .select_from(languoid_country.join(Country))\
                .where(languoid_country.c.languoid_id == Languoid.id)\
                .order_by(Country.id)\
                .label('countries')

    links = select([group_concat(Link.printf()).label('links')])\
            .where(Link.languoid_id == Languoid.id)\
            .order_by(Link.ord)\
            .label('links')

    source_gl = aliased(Source, name='source_glottolog')
    s_bibfile = aliased(Bibfile, name='source_bibfile')
    s_bibitem = aliased(Bibitem, name='source_bibitem')

    sources_glottolog = select([source_gl.printf(s_bibfile, s_bibitem)])\
                        .where(source_gl.provider == 'glottolog')\
                        .where(source_gl.languoid_id == Languoid.id)\
                        .correlate(Languoid)\
                        .where(source_gl.bibitem_id == s_bibitem.id)\
                        .where(s_bibitem.bibfile_id == s_bibfile.id)\
                        .order_by(s_bibfile.name, s_bibitem.bibkey)

    sources_glottolog = select([group_concat(sources_glottolog.c.printf)
                                .label('sources_glottolog')])\
                        .label('sources_glottolog')

    altnames = {p: aliased(Altname, name='altname_' + p) for p in sorted(ALTNAME_PROVIDER)}

    altnames = [select([group_concat(a.printf()).label('altnames_' + p)])
                .where(a.provider == p)
                .where(a.languoid_id == Languoid.id)
                .order_by(a.name, a.lang)
                .label('altnames_' + p) for p, a in altnames.items()]

    triggers = {f: aliased(Trigger, name='trigger_' + f) for f in ('lgcode', 'inlg')}

    triggers = [select([group_concat(t.trigger).label('triggers_' + f)])
                .where(t.field == f)
                .where(t.languoid_id == Languoid.id)
                .order_by(t.ord)
                .label('triggers_' + f) for f, t in triggers.items()]

    idents = {s: aliased(Identifier, name='ident_' + s) for s in sorted(IDENTIFIER_SITE)}

    identifiers = [i.identifier.label('identifier_' + s) for s, i in idents.items()]

    def crefs(kind):
        ref = aliased(ClassificationRef, name='cr_' + kind)
        r_bibfile = aliased(Bibfile, name='bibfile_cr_' + kind)
        r_bibitem = aliased(Bibitem, name='bibitem_cr_' + kind)

        refs = select([ref.printf(r_bibfile, r_bibitem)])\
               .where(ref.kind == kind)\
               .where(ref.languoid_id == Languoid.id)\
               .correlate(Languoid)\
               .where(ref.bibitem_id == r_bibitem.id)\
               .where(r_bibitem.bibfile_id == r_bibfile.id)\
               .order_by(ref.ord)

        label = f'classification_{kind}refs'
        return select([group_concat(refs.c.printf).label(label)]).label(label)

    classification_subrefs, classification_familyrefs = map(crefs, ('sub', 'family'))

    e_bibfile = aliased(Bibfile, name='bibfile_e')
    e_bibitem = aliased(Bibitem, name='bibitem_e')

    endangerment_source = EndangermentSource.printf(e_bibfile, e_bibitem)\
                          .label('endangerment_source')

    iso_retirement_change_to = select([group_concat(IsoRetirementChangeTo.code)
                                       .label('iso_retirement_change_to')])\
                               .where(IsoRetirementChangeTo.languoid_id
                                      == Languoid.id)\
                               .order_by(IsoRetirementChangeTo.ord)\
                               .label('iso_retirement_change_to')

    def get_cols(model, label='{name}', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = '_' + ignore
            cols = [c for c in cols if c.name != ignore
                    and not c.name.endswith(ignore_suffix)]
        return [c.label(label.format(name=c.name)) for c in cols]

    subc, famc = (aliased(ClassificationComment, name='cc_' + n) for n in ('sub', 'fam'))

    classification_sub = subc.comment.label('classification_sub')
    classification_family = famc.comment.label('classification_family')

    classification = [classification_sub, classification_subrefs,
                      classification_family, classification_familyrefs]

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
            ] + altnames
            + triggers
            + identifiers
            + classification
            + get_cols(Endangerment, label='endangerment_{name}') + [
            endangerment_source,
            ] + get_cols(EthnologueComment, label='elcomment_{name}')
            + get_cols(IsoRetirement, label='iso_retirement_{name}') + [
            iso_retirement_change_to,
        ], bind=bind)

    froms = Languoid.__table__
    for s, i in idents.items():
        froms = froms.outerjoin(i, sa.and_(i.site == s,
                                           i.languoid_id == Languoid.id))
    froms = froms.outerjoin(subc, sa.and_(subc.kind == 'sub',
                                          subc.languoid_id == Languoid.id))\
            .outerjoin(famc, sa.and_(famc.kind == 'family',
                                     famc.languoid_id == Languoid.id))\
            .outerjoin(sa.join(Endangerment, EndangermentSource)
                       .outerjoin(sa.join(e_bibitem, e_bibfile)))\
            .outerjoin(EthnologueComment)\
            .outerjoin(IsoRetirement)

    select_languoid.append_from(froms)

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


def write_json_query_csv(filename=None, *, ordered='id', raw=False, bind=ENGINE):
    if filename is None:
        filename = f'{__package__}.languoids-json_query.csv'

    query = get_json_query(ordered=ordered, load_json=raw, bind=bind)
    return write_csv(query, filename=filename)


@_views.register_view('path_json', load_json=False)
def get_json_query(*, ordered='id', load_json=True, bind=ENGINE):
    json_object = sa.func.json_object
    group_array = sa.func.json_group_array
    group_object = sa.func.json_group_object

    macroareas = select([group_array(languoid_macroarea.c.macroarea_name)
                         .label('macroareas')])\
                 .where(languoid_macroarea.c.languoid_id == Languoid.id)\
                 .order_by(languoid_macroarea)\
                 .as_scalar()

    countries = select([Country.jsonf()])\
                .select_from(languoid_country.join(Country))\
                .where(languoid_country.c.languoid_id == Languoid.id)\
                .correlate(Languoid)\
                .order_by(Country.printf())

    countries = select([group_array(sa.func.json(countries.c.jsonf))
                        .label('countries')]).as_scalar()

    links = select([Link.jsonf()])\
            .where(Link.languoid_id == Languoid.id)\
            .correlate(Languoid)\
            .order_by(Link.ord)

    links = select([group_array(links.c.jsonf).label('links')]).as_scalar()

    s_bibfile = aliased(Bibfile, name='source_bibfile')
    s_bibitem = aliased(Bibitem, name='source_bibitem')

    sources = select([Source.provider,
                      Source.jsonf(s_bibfile, s_bibitem)])\
             .where(Source.languoid_id == Languoid.id)\
             .correlate(Languoid)\
             .where(Source.bibitem_id == s_bibitem.id)\
             .where(s_bibitem.bibfile_id == s_bibfile.id)\
             .order_by(Source.provider, s_bibfile.name, s_bibitem.bibkey)

    sources = select([
            sources.c.provider.label('key'),
            group_array(sa.func.json(sources.c.jsonf)).label('value'),
        ]).group_by(sources.c.provider)

    sources = select([
        sa.func.nullif(group_object(sources.c.key,
                                    sa.func.json(sources.c.value)),
                       '{}').label('sources')]).as_scalar()

    altnames = select([Altname.provider,
                       Altname.jsonf()])\
               .where(Altname.languoid_id == Languoid.id)\
               .correlate(Languoid)\
               .order_by('provider', Altname.printf())

    altnames = select([
            altnames.c.provider.label('key'),
            group_array(sa.func.json(altnames.c.jsonf)).label('value'),
        ]).group_by(altnames.c.provider)

    altnames = select([
        sa.func.nullif(group_object(altnames.c.key,
                                    sa.func.json(altnames.c.value)),
                       '{}').label('altnames')]).as_scalar()

    triggers = select([Trigger.field,
                       Trigger.trigger])\
               .where(Trigger.languoid_id == Languoid.id)\
               .correlate(Languoid)\
               .order_by(Trigger.field, Trigger.ord)

    triggers = select([triggers.c.field.label('key'),
                       group_array(triggers.c.trigger).label('value')])\
               .group_by(triggers.c.field)

    triggers = select([
        sa.func.nullif(group_object(triggers.c.key,
                                    triggers.c.value),
                       '{}').label('triggers')]).as_scalar()

    identifier = select([
        sa.func.nullif(group_object(Identifier.site,
                                    Identifier.identifier),
                       '{}').label('identifier')
        ]).where(Identifier.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    classification_comment = select([
            ClassificationComment.kind.label('key'),
            ClassificationComment.comment.label('value'),
        ]).where(ClassificationComment.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    cr_bibfile = aliased(Bibfile, name='bibfile_cr')
    cr_bibitem = aliased(Bibitem, name='bibitem_cr')

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

    classification = select([
        sa.func.nullif(group_object(classification.c.key,
                                    classification.c.value),
                       '{}').label('classification')])\
                      .select_from(classification)\
                      .as_scalar()

    e_bibfile = aliased(Bibfile, name='bibfile_e')
    e_bibitem = aliased(Bibitem, name='bibitem_e')

    endangerment = select([Endangerment.jsonf(EndangermentSource,
                                              e_bibfile, e_bibitem,
                                              label='endangerment')])\
        .select_from(sa.join(Endangerment, EndangermentSource)
                     .outerjoin(sa.join(e_bibitem, e_bibfile)))\
        .where(Endangerment.languoid_id == Languoid.id)\
        .correlate(Languoid)\
        .as_scalar()

    hh_ethnologue_comment = select([EthnologueComment
                                    .jsonf(label='hh_ethnologue_comment')])\
                            .where(EthnologueComment.languoid_id
                                   == Languoid.id)\
                            .correlate(Languoid)\
                            .as_scalar()

    irct = aliased(IsoRetirementChangeTo, name='irct')

    change_to = select([irct.code])\
                .where(irct.languoid_id == IsoRetirement.languoid_id)\
                .correlate(IsoRetirement)\
                .order_by(irct.ord)

    change_to = select([group_array(change_to.c.code)
                        .label('change_to')]).as_scalar()

    iso_retirement = select([IsoRetirement.jsonf(change_to=change_to,
                                                 optional=True,
                                                 label='iso_retirement')])\
                     .where(IsoRetirement.languoid_id == Languoid.id)\
                     .correlate(Languoid)\
                     .as_scalar()

    path = Languoid.path()

    languoid = json_object('id', Languoid.id,
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
                           'iso_retirement', iso_retirement)

    if load_json:
        languoid = sa.type_coerce(languoid, sa.types.JSON)

    select_languoid = select([path, languoid.label('json')],
                             bind=bind).select_from(Languoid)

    _apply_ordered(select_languoid, path, ordered=ordered)

    return select_languoid


def print_languoid_stats(*, bind=ENGINE):
    select_stats = get_stats_query(bind=bind)
    rows, counts = itertools.tee(select_stats.execute())

    print_rows(rows, format_='{n:6,d} {kind}', bind=bind)

    sums = [('languoids', ('families', 'languages', 'subfamilies', 'dialects')),
            ('roots', ('families', 'isolates')),
            ('All', ('Spoken L1 Languages',) + SPECIAL_FAMILIES),
            ('languages', ('All', BOOKKEEPING))]

    counts = dict(counts)
    for total, parts in sums:
        values = [counts[p] for p in parts]
        parts_sum = sum(values)
        term = ' + '.join(f'{v:,d} {p}' for p, v in zip(parts, values))
        log.debug('verify %s == %d %s', term, counts[total], total)
        if counts[total] != parts_sum:  # pragma: no cover
            warnings.warn(f'{term} = {parts_sum:,d}'
                          f' (expected {counts[total]:,d} {total})')


@_views.register_view('stats')
def get_stats_query(*, bind=ENGINE):
    # cf. https://glottolog.org/glottolog/glottologinformation

    def languoid_count(kind, fromclause=Languoid):
        return select([sa.literal(kind).label('kind'),
                       sa.func.count().label('n')]).select_from(fromclause)

    Child, Root, child_root = Languoid.child_root(innerjoin=False)

    def iterselects():
        yield languoid_count('languoids')

        yield languoid_count('families').where(Languoid.level == FAMILY)\
            .where(Languoid.parent_id == None)

        yield languoid_count('isolates').where(Languoid.level == LANGUAGE)\
            .where(Languoid.parent_id == None)

        yield languoid_count('roots').where(Languoid.parent_id == None)

        yield languoid_count('languages').where(Languoid.level == LANGUAGE)

        yield languoid_count('subfamilies').where(Languoid.level == FAMILY)\
            .where(Languoid.parent_id != None)

        yield languoid_count('dialects').where(Languoid.level == DIALECT)

        other = SPECIAL_FAMILIES + (BOOKKEEPING,)
        yield languoid_count('Spoken L1 Languages', child_root)\
            .where(Child.level == LANGUAGE)\
            .where(sa.or_(Root.name == None, Root.name.notin_(other)))

        for name in SPECIAL_FAMILIES:
            yield languoid_count(name, child_root)\
                .where(Child.level == LANGUAGE)\
                .where(Root.name == name)

        # TODO: the following does not work with literal_binds
        #       .where(Family.name.is_distinct_from(BOOKKEEPING))
        yield languoid_count('All', child_root)\
            .where(Child.level == LANGUAGE)\
            .where(Root.name.op('IS NOT')(BOOKKEEPING))

        yield languoid_count(BOOKKEEPING, child_root)\
            .where(Child.level == LANGUAGE)\
            .where(Root.name == BOOKKEEPING)

    return sa.union_all(*iterselects(), bind=bind)


def iterdescendants(parent_level=None, child_level=None, *, bind=ENGINE):
    """Yield pairs of (parent id, sorted list of their descendant ids)."""
    # TODO: implement ancestors/descendants as sa.orm.relationship()
    # see https://bitbucket.org/zzzeek/sqlalchemy/issues/4165
    tree = Languoid.tree()

    Parent, Child = (aliased(Languoid, name=n) for n in ('parent', 'child'))

    child_tree = sa.join(Child, tree, tree.c.child_id == Child.id)

    parent_child = sa.outerjoin(Parent, child_tree,
                                tree.c.parent_id == Parent.id)

    select_pairs = select([Parent.id.label('parent_id'),
                           Child.id.label('child_id')], bind=bind)\
                   .select_from(parent_child)\
                   .order_by('parent_id', 'child_id')

    if parent_level is not None:
        if parent_level == 'top':
            select_pairs.append_whereclause(Parent.parent_id == None)
        elif parent_level in LEVEL:
            select_pairs.append_whereclause(Parent.level == parent_level)
        else:
            raise ValueError(f'invalid parent_level: {parent_level!r}')

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
