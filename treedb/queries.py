"""Batteries-included ``sqlalchemy`` queries for SQLite3 database."""

import functools
import logging
import typing

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import aliased

from . import _globals
from . import _tools
from . import backend as _backend
from .backend import views as _views
from . import models
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
           'get_example_query',
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
            cond = ((cls.parent == sa.null()) if is_root
                    else (cls.parent != sa.null()))
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


def _add_order_by(select_languoid, *, order_by: str, column_for_path_order):
    if order_by in (True, None, 'id'):
        select_languoid = select_languoid.order_by(Languoid.id)
    elif order_by == 'path':
        select_languoid = select_languoid.order_by(column_for_path_order)
    elif order_by is False:  # pragma: no cover
        pass
    else:  # pragma: no cover
        raise ValueError(f'order_by={order_by!r} not implemented')
    return select_languoid


def group_concat(x, *, separator: str = ', '):
    return sa.func.group_concat(x, separator)


@_views.register_view('example')
def get_example_query(*, order_by: str = 'id'):
    """Return example sqlalchemy core query (one denormalized row per languoid)."""
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
                              Languoid.longitude,
                              select_languoid_macroareas(as_json=False),
                              select_languoid_countries(as_json=False),
                              select_languoid_links(as_json=False),
                              select_languoid_sources(as_json=False,
                                                      provider_name='glottolog'))
                       .select_from(Languoid))

    select_languoid = _add_order_by(select_languoid,
                                    order_by=order_by,
                                    column_for_path_order=path)

    def select_altnames(provider_name: str) -> sa.sql.Select:
        altname = aliased(Altname, name=f'altname_{provider_name}')
        provider = aliased(AltnameProvider, name=f'altname_{provider_name}_provider')

        altnames = (select(altname.printf())
                    .select_from(altname)
                    .filter_by(languoid_id=Languoid.id)
                    .correlate(Languoid)
                    .join(altname.provider.of_type(provider))
                    .where(provider.name == provider_name)
                    .order_by(altname.name, altname.lang)
                    .alias(f'lang_altname_{provider_name}'))

        label = f'altnames_{provider_name}'
        return select(group_concat(altnames.c.printf).label(label)).label(label)

    altnames = list(map(select_altnames, sorted(ALTNAME_PROVIDER)))
    select_languoid = select_languoid.add_columns(*altnames)

    def select_triggers(field: str) -> sa.sql.Select:
        trigger = aliased(Trigger, name=f'trigger_{field}')

        triggers = (select(trigger.trigger)
                    .select_from(trigger)
                    .filter_by(field=field)
                    .filter_by(languoid_id=Languoid.id)
                    .correlate(Languoid)
                    .order_by(trigger.ord)
                    .alias(f'lang_trigger_{field}'))

        label = f'triggers_{field}'
        return select(group_concat(triggers.c.trigger).label(label)).label(label)

    triggers = list(map(select_triggers, ('lgcode', 'inlg')))
    select_languoid = select_languoid.add_columns(*triggers)

    def add_identifiers(site_name: str):
        nonlocal select_languoid

        identifier = aliased(Identifier, name=f'ident_{site_name}')
        site = aliased(IdentifierSite, name=f'ident_{site_name}_site')

        site_identifier = identifier.identifier.label(f'identifier_{site_name}')
        select_languoid = (select_languoid.add_columns(site_identifier)
                           .outerjoin(sa.join(identifier, site, identifier.site_id == site.id),
                                      sa.and_(site.name == site_name,
                                              identifier.languoid_id == Languoid.id)))

    for site_name in sorted(IDENTIFIER_SITE):
        add_identifiers(site_name)

    def add_classification(kind: str):
        nonlocal select_languoid

        comment = aliased(ClassificationComment, name=f'cc_{kind}')

        kind_comment = comment.comment.label(f'classification_{kind}')
        select_languoid = (select_languoid.add_columns(kind_comment)
                           .outerjoin(comment,
                                      sa.and_(comment.kind == kind,
                                              comment.languoid_id == Languoid.id)))

        ref = aliased(ClassificationRef, name=f'cr_{kind}')
        bibfile = aliased(Bibfile, name=f'bibfile_cr_{kind}')
        bibitem = aliased(Bibitem, name=f'bibitem_cr_{kind}')

        refs = (select(ref.printf(bibfile, bibitem))
                .select_from(ref)
                .filter_by(kind=kind)
                .filter_by(languoid_id=Languoid.id)
                .correlate(Languoid)
                .join(ref.bibitem.of_type(bibitem))
                .join(bibitem.bibfile.of_type(bibfile))
                .order_by(ref.ord)
                .alias(f'lang_cref_{kind}'))

        label = f'classification_{kind}refs'
        refs = select(group_concat(refs.c.printf).label(label)).label(label)
        select_languoid = select_languoid.add_columns(refs)

    for kind in ('sub', 'family'):
        add_classification(kind)

    def get_cols(model, label='{name}', ignore='id'):
        cols = model.__table__.columns
        if ignore:
            ignore_suffix = '_' + ignore
            cols = [c for c in cols
                    if c.name != ignore and not c.name.endswith(ignore_suffix)]
        return [c.label(label.format(name=c.name)) for c in cols]

    endangerment = get_cols(Endangerment, label='endangerment_{name}')
    select_languoid = select_languoid.add_columns(*endangerment)

    e_bibfile = aliased(Bibfile, name='bibfile_e')
    e_bibitem = aliased(Bibitem, name='bibitem_e')

    endangermentsource = (EndangermentSource.printf(e_bibfile, e_bibitem)
                          .label('endangerment_source'))

    select_languoid = (select_languoid.add_columns(endangermentsource)
                       .outerjoin(sa.join(Endangerment, EndangermentSource))
                       .outerjoin(sa.join(e_bibitem, e_bibfile)))


    ethnologue_comment = get_cols(EthnologueComment, label='elcomment_{name}')
    select_languoid = (select_languoid.add_columns(*ethnologue_comment)
                       .outerjoin(EthnologueComment))

    iso_retirement = get_cols(IsoRetirement, label='iso_retirement_{name}')
    select_languoid = (select_languoid.add_columns(*iso_retirement)
                       .outerjoin(IsoRetirement))

    iso_retirement_change_to = (select(IsoRetirementChangeTo.code)
                                .select_from(IsoRetirementChangeTo)
                                .filter_by(languoid_id=Languoid.id)
                                .correlate(Languoid)
                                .order_by(IsoRetirementChangeTo.ord)
                                .alias('lang_irct'))

    code = group_concat(iso_retirement_change_to.c.code)
    label = 'iso_retirement_change_to'
    iso_retirement_change_to = select(code.label(label)).label(label)
    select_languoid = select_languoid.add_columns(iso_retirement_change_to)

    return select_languoid


# Windows, Python < 3.9: https://www.sqlite.org/download.html
group_array = sa.func.json_group_array


group_object = sa.func.json_group_object


@_views.register_view('path_languoid', as_rows=True, load_json=False)
def get_json_query(*, limit: typing.Optional[int] = None,
                   offset: typing.Optional[int] = 0,
                   order_by: str = _globals.LANGUOID_ORDER,
                   as_rows: bool = False,
                   load_json: bool = True,
                   sort_keys: bool = False,
                   path_label: str = _globals.PATH_LABEL,
                   languoid_label: str = _globals.LANGUOID_LABEL):
    languoid = {'id': Languoid.id,
                'parent_id': Languoid.parent_id,
                'level': Languoid.level,
                'name': Languoid.name,
                'hid': Languoid.hid,
                'iso639_3': Languoid.iso639_3,
                'latitude': Languoid.latitude,
                'longitude': Languoid.longitude,
                'macroareas': select_languoid_macroareas(as_json=True),
                'countries': select_languoid_countries(as_json=True, sort_keys=sort_keys),
                'links': select_languoid_links(as_json=True, sort_keys=sort_keys),
                'timespan': select_languoid_timespan(sort_keys=sort_keys),
                'sources': select_languoid_sources(as_json=True, sort_keys=sort_keys),
                'altnames': select_languoid_altnames(sort_keys=sort_keys),
                'triggers': select_languoid_triggers(),
                'identifier': select_languoid_identifier(),
                'classification': select_languoid_classification(sort_keys=sort_keys),
                'endangerment': select_languoid_endangerment(sort_keys=sort_keys),
                'hh_ethnologue_comment': select_languoid_hh_ethnologue_comment(sort_keys=sort_keys),
                'iso_retirement': select_languoid_iso_retirement(sort_keys=sort_keys)}

    json_object = functools.partial(models.json_object, sort_keys_=sort_keys)
    del sort_keys

    value = json_object(**languoid)

    if as_rows:
        path = column_for_path_order = Languoid.path()
        if load_json:
            value = sa.type_coerce(value, sa.JSON)
        columns = [path.label(path_label), value.label(languoid_label)]
    else:
        subquery = Languoid._path_part(include_self=True, bottomup=False)

        path_array = (sa.func.json_group_array(subquery.c.path_part)
                     .label('path_array'))
        path_array = select(path_array).label('path')

        file_path = (sa.func.group_concat(subquery.c.path_part,
                                          _globals.FILE_PATH_SEP)
                     .label('path_string'))
        file_path = select(file_path).label('file_path')

        value = json_object(**{path_label: path_array,
                               languoid_label: value})
        if load_json:
            value = sa.type_coerce(value, sa.JSON)
        columns = [value]
        column_for_path_order = file_path

    select_json = select(*columns).select_from(Languoid)
    select_json = _add_order_by(select_json,
                                order_by=order_by,
                                column_for_path_order=column_for_path_order)

    if offset:
        select_json = select_json.offset(offset)
    if limit is not None:
        select_json = select_json.limit(limit)
    return select_json


def select_languoid_macroareas(languoid=Languoid,*, as_json: bool,
                               label: str = 'macroareas',
                               alias: str = 'lang_ma'):
    name = languoid_macroarea.c.macroarea_name

    macroareas = (select(name)
                  .select_from(languoid_macroarea)
                  .filter_by(languoid_id=languoid.id)
                  .correlate(languoid)
                  .order_by(name)
                  .alias(alias))

    aggregate = group_array if as_json else group_concat

    macroareas = aggregate(macroareas.c.macroarea_name)

    return select(macroareas.label(label)).label(label)


def select_languoid_countries(languoid=Languoid, *, as_json: bool,
                              label: str = 'countries',
                              sort_keys: bool = False,
                              alias: str = 'lang_country'):
    field = (Country.jsonf(sort_keys=sort_keys) if as_json else
             languoid_country.c.country_id)

    countries = (select(field)
                 .select_from(languoid_country)
                 .filter_by(languoid_id=languoid.id)
                 .correlate(languoid))

    if as_json:
        countries = (countries.join(Country)
                     .order_by(Country.printf())
                     .alias(alias))

        countries = group_array(sa.func.json(countries.c.jsonf))
    else:
        countries = (countries
                     .order_by(field)
                     .alias(alias))

        countries = group_concat(countries.c.country_id)

    return select(countries.label(label)).label(label)


def select_languoid_links(languoid=Languoid, *, as_json: bool,
                          label: str = 'links',
                          sort_keys: bool = False,
                          alias: str = 'lang_link'):
    links = (select(Link.jsonf(sort_keys=sort_keys) if as_json else
                    Link.printf())
             .select_from(Link)
             .filter_by(languoid_id=languoid.id)
             .correlate(languoid)
             .order_by(Link.ord)
             .alias(alias))

    links = group_array(links.c.jsonf) if as_json else group_concat(links.c.printf)

    return select(links.label(label)).label(label)


def select_languoid_timespan(languoid=Languoid,
                             *, label: str = 'timespan',
                             sort_keys: bool = False):
    return (select(Timespan.jsonf(sort_keys=sort_keys))
            .select_from(Timespan)
            .filter_by(languoid_id=languoid.id)
            .correlate(languoid)
            .label(label))


def select_languoid_sources(languoid=Languoid, *, as_json: bool,
                            provider_name: typing.Optional[str] = None,
                            label: str = 'sources',
                            sort_keys: bool = False,
                            alias: str = 'lang_source'):
    source = (aliased(Source, name=f'source_{provider_name}')
              if provider_name is not None else Source)

    provider = aliased(SourceProvider, name='source_provider')

    bibitem = aliased(Bibitem, name='source_bibitem')
    bibfile = aliased(Bibfile, name='source_bibfile')

    columns = [source.jsonf(bibfile, bibitem, sort_keys=sort_keys)
               if as_json else source.printf(bibfile, bibitem)]

    order_by = [bibfile.name, bibitem.bibkey]

    if provider_name is None:
        name = provider.name
        columns.insert(0, name.label('provider'))
        order_by.insert(0, name)

    sources = (select(*columns)
               .select_from(source)
               .filter_by(languoid_id=languoid.id)
               .correlate(languoid)
               .join(Source.provider.of_type(provider))
               .join(Source.bibitem.of_type(bibitem))
               .join(bibitem.bibfile.of_type(bibfile))
               .order_by(*order_by))

    if provider_name is not None:
        sources = sources.where(provider.name == provider_name)
        alias = f'{alias}_{provider_name}'

    sources = sources.alias(alias)

    sub_label = f'{label}_{provider_name}' if provider_name else label

    field = (group_array(sa.func.json(sources.c.jsonf)).label('value')
             if as_json else group_concat(sources.c.printf).label(sub_label))

    if provider_name is not None:
        return select(field).label(sub_label)

    key = sources.c.provider

    sources = (select(key.label('key'), field)
               .group_by(key)
               .alias(alias))

    if as_json:
        sources = sa.func.nullif(group_object(sources.c.key,
                                              sa.func.json(sources.c.value)),
                                 '{}')
    else:  # pragma: no cover
        raise NotImplementedError

    return select(sources.label(label)).label(label)


def select_languoid_altnames(languoid=Languoid,
                             *, label: str = 'altnames',
                             sort_keys: bool = False,
                             alias: str = 'lang_altname'):
    provider = aliased(AltnameProvider, name='altname_provider')

    altnames = (select(provider.name.label('provider'),
                       Altname.jsonf(sort_keys=sort_keys))
                .select_from(Altname)
                .filter_by(languoid_id=languoid.id)
                .correlate(languoid)
                .join(Altname.provider.of_type(provider))
                .order_by(provider.name, Altname.printf())
                .alias(alias))

    altnames = (select(altnames.c.provider.label('key'),
                       group_array(sa.func.json(altnames.c.jsonf))
                       .label('value'))
                .group_by(altnames.c.provider)
                .alias(alias))

    altnames = sa.func.nullif(group_object(altnames.c.key,
                                           sa.func.json(altnames.c.value)),
                              '{}')

    return select(altnames.label(label)).label(label)


def select_languoid_triggers(languoid=Languoid,
                             *, label: str = 'triggers'):
    field = Trigger.field

    triggers = (select(field, Trigger.trigger)
                .select_from(Trigger)
                .filter_by(languoid_id=languoid.id)
                .correlate(languoid)
                .order_by(field, Trigger.ord)
                .alias('lang_trigger'))

    triggers = (select(triggers.c.field.label('key'),
                       group_array(triggers.c.trigger).label('value'))
                .group_by(triggers.c.field)
                .alias('lang_triggers'))

    triggers = sa.func.nullif(group_object(triggers.c.key,
                                           triggers.c.value),
                              '{}')

    return select(triggers.label(label)).label(label)


def select_languoid_identifier(languoid=Languoid,
                               *, label: str = 'identifiers'):
    identifier = (select(IdentifierSite.name.label('site'),
                         Identifier.identifier.label('identifier'))
                  .select_from(Identifier)
                  .filter_by(languoid_id=languoid.id)
                  .correlate(languoid)
                  .join(Identifier.site.of_type(IdentifierSite))
                  .alias('lang_identifiers'))

    site = identifier.c.site

    identifier = (select(site, identifier.c.identifier)
                  .order_by(site)
                  .alias('lang_identifiers_ordered'))

    identifier = sa.func.nullif(group_object(identifier.c.site,
                                             identifier.c.identifier),
                                '{}')

    return select(identifier.label(label)).label(label)


def select_languoid_classification(languoid=Languoid,
                                   *, label: str = 'classification',
                                   sort_keys: bool = False):
    classification_comment = (select(ClassificationComment.kind.label('key'),
                                     sa.func.json_quote(ClassificationComment.comment).label('value'))
                              .select_from(ClassificationComment)
                              .filter_by(languoid_id=languoid.id)
                              .correlate(languoid)
                              .scalar_subquery())

    bibitem = aliased(Bibitem, name='bibitem_cr')
    bibfile = aliased(Bibfile, name='bibfile_cr')

    kind = ClassificationRef.kind

    classification_refs = (select((kind + 'refs').label('key'),
                                  ClassificationRef.jsonf(bibfile, bibitem,
                                                          sort_keys=sort_keys))
                           .select_from(ClassificationRef)
                           .filter_by(languoid_id=languoid.id)
                           .correlate(languoid)
                           .join(ClassificationRef.bibitem.of_type(bibitem))
                           .join(bibitem.bibfile.of_type(bibfile))
                           .order_by(kind, ClassificationRef.ord)
                           .alias('lang_cref'))

    classification_refs = (select(classification_refs.c.key,
                                  group_array(sa.func.json(classification_refs.c.jsonf))
                                  .label('value'))
                           .group_by(classification_refs.c.key))

    classification = (classification_comment
                      .union_all(classification_refs)
                      .alias('lang_classifciation'))

    key = classification.c.key

    classification = (select(key, classification.c.value.label('value'))
                      .order_by(key)
                      .alias('classification_object'))

    nullification = sa.func.nullif(group_object(classification.c.key,
                                                sa.func.json(classification.c.value)),
                                   '{}')

    return (select(nullification.label(label))
            .select_from(classification)
            .label(label))


def select_languoid_endangerment(languoid=Languoid,
                                 *, label: str = 'endangerment',
                                 sort_keys: bool = False):
    bibitem = aliased(Bibitem, name='bibitem_e')
    bibfile = aliased(Bibfile, name='bibfile_e')

    return (select(Endangerment.jsonf(EndangermentSource,
                                      bibfile, bibitem,
                                      sort_keys=sort_keys,
                                      label=label))
            .select_from(Endangerment)
            .filter_by(languoid_id=languoid.id)
            .correlate(languoid)
            .join(Endangerment.source)
            .outerjoin(sa.join(bibitem, bibfile))
            .label(label))


def select_languoid_hh_ethnologue_comment(languoid=Languoid,
                                          *, label: str = 'hh_ethnologue_comment',
                                          sort_keys: bool = False):
    return (select(EthnologueComment
                   .jsonf(sort_keys=sort_keys, label=label))
            .select_from(EthnologueComment)
            .filter_by(languoid_id=languoid.id)
            .correlate(languoid)
            .label(label))


def select_languoid_iso_retirement(languoid=Languoid,
                                   *, label: str = 'iso_retirement',
                                   sort_keys: bool = False,
                                   alias: str = 'lang_irct',
                                   alias_label: str = 'change_to'):
    change_to = (select(IsoRetirementChangeTo.code)
                 .select_from(IsoRetirementChangeTo)
                 .filter_by(languoid_id=IsoRetirement.languoid_id)
                 .correlate(IsoRetirement)
                 .order_by(IsoRetirementChangeTo.ord)
                 .alias(alias))

    change_to = (select(group_array(change_to.c.code).label(alias_label))
                 .label(alias_label))

    return (select(IsoRetirement.jsonf(change_to=change_to,
                                       sort_keys=sort_keys,
                                       optional=True,
                                       label=label))
            .select_from(IsoRetirement)
            .filter_by(languoid_id=languoid.id)
            .correlate(languoid)
            .label(label))


def iterdescendants(parent_level: typing.Optional[str] = None,
                    child_level: typing.Optional[str] = None,
                    *, bind=_globals.ENGINE):
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
    else:  # pragma: no cover
        raise ValueError(f'invalid parent_level: {parent_level!r}')

    Parent, Child, parent_child = Languoid.parent_descendant(parent_root=parent_root,
                                                             parent_level=parent_level)

    select_pairs = (select(Parent.id.label('parent_id'),
                           Child.id.label('child_id'))
                    .select_from(parent_child)
                    .order_by('parent_id', 'child_id'))

    if child_level is not None:
        if child_level not in LEVEL:  # pragma: no cover
            raise ValueError(f'invalid child_level: {child_level!r}')
        select_pairs = select_pairs.where(sa.or_(Child.level == sa.null(),
                                                 Child.level == child_level))

    rows = _backend.iterrows(select_pairs, bind=bind)

    for parent_id, grp in _tools.groupby_attrgetter('parent_id')(rows):
        _, c = next(grp)
        if c is None:
            descendants = []
        else:
            descendants = [c] + [c for _, c in grp]
        yield parent_id, descendants
