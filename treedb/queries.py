# queries.py - batteries-included sqlalchemy queries for sqlite3 db

import functools
import logging

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import aliased

from . import ENGINE

from . import _tools
from . import backend as _backend
from .backend import views as _views
from .models import (LEVEL, FAMILY, LANGUAGE, DIALECT,
                     SPECIAL_FAMILIES, BOOKKEEPING,
                     ALTNAME_PROVIDER, IDENTIFIER_SITE,
                     Languoid,
                     languoid_macroarea,
                     languoid_country, Country,
                     Link, Source, SourceProvider, Timespan, Bibfile, Bibitem,
                     Altname, AltnameProvider, Trigger,
                     Identifier, IdentifierSite,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['get_stats_query',
           'get_query',
           'get_json_query',
           'iterdescendants']


log = logging.getLogger(__name__)


@_views.register_view('stats')
def get_stats_query():
    # cf. https://glottolog.org/glottolog/glottologinformation

    def languoid_count(kind, cls=Languoid, fromclause=Languoid,
                       level=None, is_root=None):
        select_nrows = (select(sa.literal(kind).label('kind'),
                               sa.func.count().label('n'))
                        .select_from(fromclause))

        if level is not None:
            select_nrows = select_nrows.where(cls.level == level)

        if is_root is not None:
            cond = (cls.parent == None) if is_root else (cls.parent != None)
            select_nrows = select_nrows.where(cond)

        return select_nrows

    Root, Child, root_child = Languoid.parent_descendant(innerjoin='reflexive',
                                                         parent_root=True)

    language_count = functools.partial(languoid_count,
                                       cls=Child, fromclause=root_child,
                                       level=LANGUAGE)

    def iterselects():
        yield languoid_count('languoids')

        yield languoid_count('families', level=FAMILY, is_root=True)

        yield languoid_count('isolates', level=LANGUAGE, is_root=True)

        yield languoid_count('roots', is_root=True)

        yield languoid_count('languages', level=LANGUAGE)

        yield languoid_count('subfamilies', level=FAMILY, is_root=False)

        yield languoid_count('dialects', level=DIALECT)

        yield (language_count('Spoken L1 Languages')
               .where(Root.name.notin_(SPECIAL_FAMILIES + (BOOKKEEPING,))))

        for name in SPECIAL_FAMILIES:
            yield language_count(name).where(Root.name == name)

        yield language_count('All').where(Root.name != BOOKKEEPING)

        yield language_count(BOOKKEEPING).where(Root.name == BOOKKEEPING)

    return sa.union_all(*iterselects())


def _ordered_by(select_languoid, path, *, ordered):
    if ordered is False:
        pass
    elif ordered in (True, 'id'):
        select_languoid = select_languoid.order_by(Languoid.id)
    elif ordered == 'path':
        select_languoid = select_languoid.order_by(path)
    else:
        raise ValueError(f'ordered={ordered!r} not implemented')
    return select_languoid


@_views.register_view('example')
def get_query(*, ordered='id', separator=', '):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
    group_concat = lambda x: sa.func.group_concat(x, separator)

    path, family, language = Languoid.path_family_language()

    select_languoid = (select(Languoid.id,
                              Languoid.name,
                              Languoid.level,
                              Languoid.parent_id,
                              path.label('path'),
                              family.label('family_id'),
                              language.label('dialect_language_id'),
                              Languoid.hid,
                              Languoid.iso639_3,
                              Languoid.latitude,
                              Languoid.longitude)
                       .select_from(Languoid))

    select_languoid = _ordered_by(select_languoid, path, ordered=ordered)

    macroareas = (select(languoid_macroarea.c.macroarea_name)
                  .select_from(languoid_macroarea)
                  .filter_by(languoid_id=Languoid.id)
                  .correlate(Languoid)
                  .order_by('macroarea_name')
                  .alias('lang_ma'))

    macroareas = (select(group_concat(macroareas.c.macroarea_name)
                         .label('macroareas'))
                  .label('macroareas'))

    select_languoid = select_languoid.add_columns(macroareas)

    countries = (select(languoid_country.c.country_id)
                 .select_from(languoid_country)
                 .filter_by(languoid_id=Languoid.id)
                 .correlate(Languoid)
                 .order_by('country_id')
                 .alias('lang_country'))

    countries = (select(group_concat(countries.c.country_id).label('countries'))
                 .label('countries'))

    select_languoid = select_languoid.add_columns(countries)

    links = (select(Link.printf())
             .select_from(Link)
             .filter_by(languoid_id=Languoid.id)
             .correlate(Languoid)
             .order_by(Link.ord)
             .alias('lang_link'))

    links = (select(group_concat(links.c.printf).label('links'))
             .label('links'))

    select_languoid = select_languoid.add_columns(links)

    source_gl = aliased(Source, name='source_glottolog')
    s_provider = aliased(SourceProvider, name='source_provider')
    s_bibfile = aliased(Bibfile, name='source_bibfile')
    s_bibitem = aliased(Bibitem, name='source_bibitem')

    sources_glottolog = (select(source_gl.printf(s_bibfile, s_bibitem))
                         .select_from(source_gl)
                         .filter_by(languoid_id=Languoid.id)
                         .correlate(Languoid)
                         .filter_by(provider_id=s_provider.id)
                         .where(s_provider.name == 'glottolog')
                         .filter_by(bibitem_id=s_bibitem.id)
                         .where(s_bibitem.bibfile_id == s_bibfile.id)
                         .order_by(s_bibfile.name, s_bibitem.bibkey)
                         .alias('lang_source_glottolog'))

    sources_glottolog = (select(group_concat(sources_glottolog.c.printf)
                                .label('sources_glottolog'))
                         .label('sources_glottolog'))

    select_languoid = select_languoid.add_columns(sources_glottolog)

    altnames = {p: (aliased(Altname, name='altname_' + p),
                    aliased(AltnameProvider, name='altname_' + p + '_provider'))
                for p in sorted(ALTNAME_PROVIDER)}

    altnames = {p: (select(a.printf())
                    .select_from(a)
                    .filter_by(languoid_id=Languoid.id)
                    .correlate(Languoid)
                    .filter_by(provider_id=ap.id)
                    .where(ap.name == p)
                    .order_by(a.name, a.lang)
                    .alias('lang_altname_' + p))
                for p, (a, ap) in altnames.items()}

    altnames = [select(group_concat(a.c.printf).label('altnames_' + p))
                .label('altnames_' + p) for p, a in altnames.items()]

    select_languoid = select_languoid.add_columns(*altnames)

    triggers = {f: aliased(Trigger, name='trigger_' + f) for f in ('lgcode', 'inlg')}

    triggers = {f: (select(t.trigger)
                    .select_from(t)
                    .filter_by(field=f)
                    .filter_by(languoid_id=Languoid.id)
                    .correlate(Languoid)
                    .order_by(t.ord)
                    .alias(f'lang_trigger_{f}'))
                for f, t in triggers.items()}

    triggers = [select(group_concat(t.c.trigger).label('triggers_' + f))
                .label('triggers_' + f) for f, t in triggers.items()]

    select_languoid = select_languoid.add_columns(*triggers)

    idents = {s: (aliased(Identifier, name='ident_' + s),
                  aliased(IdentifierSite, name='ident_' + s + '_site'))
              for s in sorted(IDENTIFIER_SITE)}

    identifiers = [i.identifier.label('identifier_' + s)
                   for s, (i, _) in idents.items()]

    select_languoid = select_languoid.add_columns(*identifiers)

    for s, (i, is_) in idents.items():
        select_languoid = (select_languoid
                           .outerjoin(sa.join(i, is_, i.site_id == is_.id),
                                      sa.and_(is_.name == s,
                                              i.languoid_id == Languoid.id)))

    subc, famc = (aliased(ClassificationComment, name='cc_' + n) for n in ('sub', 'fam'))

    classification_sub = subc.comment.label('classification_sub')
    classification_family = famc.comment.label('classification_family')

    def crefs(kind):
        ref = aliased(ClassificationRef, name='cr_' + kind)
        r_bibfile = aliased(Bibfile, name='bibfile_cr_' + kind)
        r_bibitem = aliased(Bibitem, name='bibitem_cr_' + kind)

        refs = (select(ref.printf(r_bibfile, r_bibitem))
                .select_from(ref)
                .filter_by(kind=kind)
                .filter_by(languoid_id=Languoid.id)
                .correlate(Languoid)
                .filter_by(bibitem_id=r_bibitem.id)
                .where(r_bibitem.bibfile_id == r_bibfile.id)
                .order_by(ref.ord)
                .alias('lang_cref_' + kind))

        label = f'classification_{kind}refs'
        return select(group_concat(refs.c.printf).label(label)).label(label)

    classification_subrefs, classification_familyrefs = map(crefs, ('sub', 'family'))

    select_languoid = (select_languoid.add_columns(classification_sub,
                                                   classification_subrefs,
                                                   classification_family,
                                                   classification_familyrefs)
                       .outerjoin(subc, sa.and_(subc.kind == 'sub',
                                                subc.languoid_id == Languoid.id))
                       .outerjoin(famc, sa.and_(famc.kind == 'family',
                                                famc.languoid_id == Languoid.id)))

    def get_cols(model, label='{name}', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = '_' + ignore
            cols = [c for c in cols if c.name != ignore
                    and not c.name.endswith(ignore_suffix)]
        return [c.label(label.format(name=c.name)) for c in cols]

    select_languoid = (select_languoid
                       .add_columns(*get_cols(Endangerment,
                                              label='endangerment_{name}')))

    e_bibfile = aliased(Bibfile, name='bibfile_e')
    e_bibitem = aliased(Bibitem, name='bibitem_e')

    endangerment_source = (EndangermentSource.printf(e_bibfile, e_bibitem)
                           .label('endangerment_source'))

    select_languoid = (select_languoid.add_columns(endangerment_source)
                       .outerjoin(sa.join(Endangerment, EndangermentSource))
                       .outerjoin(sa.join(e_bibitem, e_bibfile)))

    select_languoid = (select_languoid
                       .add_columns(*get_cols(EthnologueComment,
                                              label='elcomment_{name}'))
                       .outerjoin(EthnologueComment))

    select_languoid = (select_languoid
                       .add_columns(*get_cols(IsoRetirement,
                                             label='iso_retirement_{name}'))
                       .outerjoin(IsoRetirement))

    iso_retirement_change_to = (select(IsoRetirementChangeTo.code)
                                .select_from(IsoRetirementChangeTo)
                                .filter_by(languoid_id=Languoid.id)
                                .correlate(Languoid)
                                .order_by(IsoRetirementChangeTo.ord)
                                .alias('lang_irct'))

    iso_retirement_change_to = (select(group_concat(iso_retirement_change_to.c.code)
                                        .label('iso_retirement_change_to'))
                                .label('iso_retirement_change_to'))

    select_languoid = select_languoid.add_columns(iso_retirement_change_to)

    return select_languoid


@_views.register_view('path_json', as_rows=True, load_json=False)
def get_json_query(*, ordered='id', as_rows=True, load_json=True,
                   path_label='path', languoid_label='json'):
    # Windows, Python < 3.9: https://www.sqlite.org/download.html
    json_object = sa.func.json_object
    group_array = sa.func.json_group_array
    group_object = sa.func.json_group_object

    languoid = {'id': Languoid.id,
               'parent_id': Languoid.parent_id,
               'level': Languoid.level,
               'name': Languoid.name,
               'hid': Languoid.hid,
               'iso639_3': Languoid.iso639_3,
               'latitude': Languoid.latitude,
               'longitude': Languoid.longitude}

    macroareas = (select(languoid_macroarea.c.macroarea_name)
                  .select_from(languoid_macroarea)
                  .filter_by(languoid_id=Languoid.id)
                  .correlate(Languoid)
                  .order_by('macroarea_name')
                  .alias('lang_ma'))

    macroareas = (select(group_array(macroareas.c.macroarea_name)
                         .label('macroareas'))
                  .scalar_subquery())

    languoid['macroareas'] = macroareas

    countries = (select(Country.jsonf())
                 .join_from(languoid_country, Country)
                 .where(languoid_country.c.languoid_id == Languoid.id)
                 .correlate(Languoid)
                 .order_by(Country.printf())
                 .alias('lang_country'))

    countries = (select(group_array(sa.func.json(countries.c.jsonf))
                        .label('countries'))
                 .scalar_subquery())

    languoid['countries'] = countries

    links = (select(Link.jsonf())
             .select_from(Link)
             .filter_by(languoid_id=Languoid.id)
             .correlate(Languoid)
             .order_by(Link.ord)
             .alias('lang_link'))

    links = (select(group_array(links.c.jsonf).label('links'))
             .scalar_subquery())

    languoid['links'] = links

    timespan = (select(Timespan.jsonf())
                .select_from(Timespan)
                .filter_by(languoid_id=Languoid.id)
                .scalar_subquery())

    languoid['timespan'] = timespan

    s_provider = aliased(SourceProvider, name='source_provider')
    s_bibfile = aliased(Bibfile, name='source_bibfile')
    s_bibitem = aliased(Bibitem, name='source_bibitem')

    sources = (select(s_provider.name.label('provider'),
                      Source.jsonf(s_bibfile, s_bibitem))
              .select_from(Source)
              .filter_by(languoid_id=Languoid.id)
              .correlate(Languoid)
              .filter_by(provider_id=s_provider.id)
              .filter_by(bibitem_id=s_bibitem.id)
              .where(s_bibitem.bibfile_id == s_bibfile.id)
              .order_by(s_provider.name, s_bibfile.name, s_bibitem.bibkey)
              .alias('lang_source'))

    sources = (select(sources.c.provider.label('key'),
                     group_array(sa.func.json(sources.c.jsonf)).label('value'))
               .group_by(sources.c.provider)
               .alias('lang_sources'))

    sources = (select(sa.func.nullif(group_object(sources.c.key,
                                     sa.func.json(sources.c.value)), '{}')
                      .label('sources'))
               .scalar_subquery())

    languoid['sources'] = sources

    a_provider = aliased(AltnameProvider, name='altname_provider')

    altnames = (select(a_provider.name.label('provider'), Altname.jsonf())
                .select_from(Altname)
                .filter_by(languoid_id=Languoid.id)
                .correlate(Languoid)
                .filter_by(provider_id=a_provider.id)
                .order_by(a_provider.name, Altname.printf())
                .alias('lang_altname'))

    altnames = (select(altnames.c.provider.label('key'),
                       group_array(sa.func.json(altnames.c.jsonf))
                       .label('value'))
                .group_by(altnames.c.provider)
                .alias('lang_altnames'))

    altnames = (select(sa.func.nullif(group_object(altnames.c.key,
                                      sa.func.json(altnames.c.value)), '{}')
                       .label('altnames'))
                .scalar_subquery())

    languoid['altnames'] = altnames

    triggers = (select(Trigger.field, Trigger.trigger)
                .select_from(Trigger)
                .filter_by(languoid_id=Languoid.id)
                .correlate(Languoid)
                .order_by('field', Trigger.ord)
                .alias('lang_trigger'))

    triggers = (select(triggers.c.field.label('key'),
                       group_array(triggers.c.trigger).label('value'))
                .group_by(triggers.c.field)
                .alias('lang_triggers'))

    triggers = (select(sa.func.nullif(group_object(triggers.c.key,
                                                   triggers.c.value),
                                      '{}').label('triggers'))
                .scalar_subquery())

    languoid['triggers'] = triggers

    identifier = (select(sa.func.nullif(group_object(IdentifierSite.name.label('site'),
                                                     Identifier.identifier),
                                        '{}').label('identifier'))
                 .select_from(Identifier)
                 .filter_by(languoid_id=Languoid.id)
                 .correlate(Languoid)
                 .filter_by(site_id=IdentifierSite.id)
                 .scalar_subquery())

    languoid['identifier'] = identifier

    classification_comment = (select(ClassificationComment.kind.label('key'),
                                     ClassificationComment.comment.label('value'))
                              .select_from(ClassificationComment)
                              .filter_by(languoid_id=Languoid.id)
                              .correlate(Languoid)
                              .scalar_subquery())

    cr_bibfile = aliased(Bibfile, name='bibfile_cr')
    cr_bibitem = aliased(Bibitem, name='bibitem_cr')

    classification_refs = (select((ClassificationRef.kind + 'refs').label('key'),
                                  ClassificationRef.jsonf(cr_bibfile, cr_bibitem))
                           .select_from(ClassificationRef)
                           .filter_by(languoid_id=Languoid.id)
                           .correlate(Languoid)
                           .filter_by(bibitem_id=cr_bibitem.id)
                           .where(cr_bibitem.bibfile_id == cr_bibfile.id)
                           .order_by(ClassificationRef.kind, ClassificationRef.ord)
                           .alias('lang_cref'))

    classification_refs = (select(classification_refs.c.key,
                                  group_array(sa.func.json(classification_refs.c.jsonf))
                                  .label('value'))
                           .group_by(classification_refs.c.key))

    classification = (classification_comment
                      .union_all(classification_refs)
                      .alias('lang_classifciation'))

    classification = (select(sa.func.nullif(group_object(classification.c.key,
                                                         classification.c.value),
                                            '{}').label('classification'))
                      .select_from(classification)
                      .scalar_subquery())

    languoid['classification'] = classification

    e_bibfile = aliased(Bibfile, name='bibfile_e')
    e_bibitem = aliased(Bibitem, name='bibitem_e')

    endangerment = (select(Endangerment.jsonf(EndangermentSource,
                                              e_bibfile, e_bibitem,
                                              label='endangerment'))
                    .join_from(Endangerment, EndangermentSource)
                    .outerjoin(sa.join(e_bibitem, e_bibfile))
                    .where(Endangerment.languoid_id == Languoid.id)
                    .correlate(Languoid)
                    .scalar_subquery())

    languoid['endangerment'] = endangerment

    hh_ethnologue_comment = (select(EthnologueComment
                                    .jsonf(label='hh_ethnologue_comment'))
                             .select_from(EthnologueComment)
                             .filter_by(languoid_id=Languoid.id)
                             .correlate(Languoid)
                             .scalar_subquery())

    languoid['hh_ethnologue_comment'] = hh_ethnologue_comment

    irct = aliased(IsoRetirementChangeTo, name='irct')

    change_to = (select(irct.code)
                 .select_from(irct)
                 .filter_by(languoid_id=IsoRetirement.languoid_id)
                 .correlate(IsoRetirement)
                 .order_by(irct.ord)
                 .alias('lang_irct'))

    change_to = (select(group_array(change_to.c.code).label('change_to'))
                 .scalar_subquery())

    iso_retirement = (select(IsoRetirement.jsonf(change_to=change_to,
                                                 optional=True,
                                                 label='iso_retirement'))
                      .select_from(IsoRetirement)
                      .filter_by(languoid_id=Languoid.id)
                      .correlate(Languoid)
                      .scalar_subquery())

    languoid['iso_retirement'] = iso_retirement

    languoid = json_object(*(x for kv in languoid.items() for x in kv))

    if as_rows:
        path = Languoid.path()

        if load_json:
            languoid = sa.type_coerce(languoid, sa.types.JSON)

        columns = [path.label(path_label),
                   languoid.label(languoid_label)]
    else:
        path = Languoid.path_json()

        path_json = json_object(path_label, path,
                                languoid_label, languoid)

        if load_json:
            path_json = sa.type_coerce(path_json, sa.types.JSON)

        columns = [path_json]

    select_json = select(*columns).select_from(Languoid)

    select_json = _ordered_by(select_json, path, ordered=ordered)

    return select_json


def iterdescendants(parent_level=None, child_level=None, *, bind=ENGINE):
    """Yield pairs of (parent id, sorted list of their descendant ids)."""
    # TODO: implement ancestors/descendants as sa.orm.relationship()
    # see https://bitbucket.org/zzzeek/sqlalchemy/issues/4165
    parent_root = None
    if parent_level is None:
        pass
    elif parent_level == 'top':
        parent_root = True
        parent_level = None
    elif parent_level in LEVEL:
        pass
    else:
        raise ValueError(f'invalid parent_level: {parent_level!r}')

    Parent, Child, parent_child = Languoid.parent_descendant(parent_root=parent_root,
                                                             parent_level=parent_level)

    select_pairs = (select(Parent.id.label('parent_id'),
                           Child.id.label('child_id'))
                    .select_from(parent_child)
                    .order_by('parent_id', 'child_id'))

    if child_level is not None:
        if child_level not in LEVEL:
            raise ValueError(f'invalid child_level: {child_level!r}')
        select_pairs = select_pairs.where(sa.or_(Child.level == None,
                                                 Child.level == child_level))

    rows = _backend.iterrows(select_pairs, bind=bind)

    for parent_id, grp in _tools.groupby_attrgetter('parent_id')(rows):
        _, c = next(grp)
        if c is None:
            descendants = []
        else:
            descendants = [c] + [c for _, c in grp]
        yield parent_id, descendants
