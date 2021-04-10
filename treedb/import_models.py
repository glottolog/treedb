# load glottolog languoid model tables

import functools
import logging
import warnings

import sqlalchemy as sa

from .backend.models import Config
from .models import (LEVEL, SPECIAL_FAMILIES, BOOKKEEPING,
                     CLASSIFICATION,
                     Languoid, LanguoidLevel, PseudoFamily,
                     languoid_macroarea, Macroarea,
                     languoid_country, Country,
                     Link, Timespan,
                     Source, SourceProvider,
                     Bibfile, Bibitem,
                     Altname, AltnameProvider,
                     Trigger, Identifier, IdentifierSite,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentStatus, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['main']


log = logging.getLogger(__name__)


class ModelMap(dict):
    """Insert model (derive params f/ key), add ``key -> primary_key`` to map."""

    model = None

    @staticmethod
    def key_to_params(key):
        return {'name': key}

    def __init__(self, items=(),
                 *, conn,
                 model=None, key_to_params=None,
                 log_insert=True):
        super().__init__(items)
        if model is not None:
            self.model = model
        if key_to_params is not None:
            self.key_to_params = key_to_params
        self.log_insert = log_insert

        self.conn = conn
        self.insert = functools.partial(conn.execute, sa.insert(self.model))

    def __missing__(self, key):
        if self.log_insert:
            log.debug('insert new %s: %r', self.model.__tablename__, key)
        params = self.key_to_params(key)

        pk, = self.insert(params).inserted_primary_key

        self[key] = pk
        return pk


def main(languoids, *, conn):

    bibfile_ids = ModelMap(conn=conn, model=Bibfile)

    class BibitemMap(ModelMap):
        # using closure over bibfile_ids

        model = Bibitem

        @staticmethod
        def key_to_params(key):
            bibfile_name, bibkey = key
            return {'bibfile_id': bibfile_ids[bibfile_name], 'bibkey': bibkey}

        def pop_params(self, params):
            return self[params.pop('bibfile'), params.pop('bibkey')]

    bibitem_ids = BibitemMap(conn=conn,
                             log_insert=False)  # silence bibitems

    class EndangermentSourceMap(ModelMap):
        # using closure over bibitem_ids

        model = EndangermentSource

        @staticmethod
        def key_to_params(key):
            params = dict(key)
            if params.get('bibfile') is not None:
                params['bibitem_id'] = bibitem_ids.pop_params(params)
            return params

        @staticmethod
        def params_to_key(params):
            return tuple(sorted(params.items()))

    insert_languoid_levels(conn)

    insert_macroareas(conn)

    insert_endangermentstatus(conn, bibitem_ids=bibitem_ids)

    es_ids = EndangermentSourceMap(conn=conn)

    insert_languoids(conn,
                     languoids=languoids,
                     bibitem_ids=bibitem_ids, es_ids=es_ids)

    insert_pseudofamilies(conn)


def insert_languoid_levels(conn, *, config_file='languoid_levels.ini'):
    log.info('insert languoid levels from: %r', config_file)
    levels = Config.load(config_file, bind=conn)
    levels = dict(sorted(levels.items(), key=lambda x: int(x[1]['ordinal'])))

    assert set(levels) >= set(LEVEL)
    extra_levels = sorted(set(levels) - set(LEVEL))
    if extra_levels:
        warnings.warn(f'{config_file!r} has extra languoid levels:'
                      f' {extra_levels!r}')

    log.debug('insert %d languoid levels: %r', len(levels), list(levels))
    params = [{'name': section,
               'description': l['description'].strip(),
               'ordinal': int(l['ordinal'])}
              for section, l in levels.items()]
    conn.execute(sa.insert(LanguoidLevel), params)


def insert_pseudofamilies(conn, *, config_file='language_types.ini'):
    log.info('insert pseudofamilies from: %r', config_file)
    languagetypes = Config.load(config_file, bind=conn)
    pseudofamilies = {section: l for section, l in languagetypes.items()
                      if l.get('pseudo_family_id', '').strip()}

    log.debug('insert %d pseudofamilies: %r', len(pseudofamilies),
              list(pseudofamilies))
    params = [{'languoid_id': p['pseudo_family_id'].strip(),
               'name': p['category'].strip(),
               'config_section': section,
               'description': p.get('description', '').strip() or None,
               'bookkeeping': p['category'].strip() == BOOKKEEPING}
              for section, p in pseudofamilies.items()]
    conn.execute(sa.insert(PseudoFamily), params)

    query = sa.select(~sa.exists()
                      .where(PseudoFamily.languoid_id == Languoid.id)
                      .where(PseudoFamily.name != Languoid.name))
    assert conn.scalar(query), 'pseudo_family_id must be in-sync with category'

    inserted_special_families = {p['name'] for p in params if not p['bookkeeping']}
    unseen_special_families = inserted_special_families - set(SPECIAL_FAMILIES)
    missing_special_families = set(SPECIAL_FAMILIES) - inserted_special_families
    if unseen_special_families:
         warnings.warn(f'{config_file!r} has extra pseudofamilies:'
                       f' {unseen_special_families!r}')
    if missing_special_families:
         warnings.warn(f'{config_file!r} misses special families:'
                       f' {missing_special_families!r}')
    inserted_bookkeeping = {p['name'] for p in params if p['bookkeeping']}
    if inserted_bookkeeping != {BOOKKEEPING}:
        warnings.warn(f'inserted bookkeeping: {inserted_bookkeeeping!r}'
                      f' expected bookkeeping: {expected_bookkeeping!r}')


def insert_macroareas(conn, *, config_file='macroareas.ini'):
    log.info('insert macroareas from: %r', config_file)
    macroareas = Config.load(config_file, bind=conn)

    log.debug('insert %d macroareas: %r', len(macroareas), list(macroareas))
    params = [{'name': m['name'].strip(),
               'config_section': section,
               'description': m['description'].strip()}
              for section, m in macroareas.items()]
    conn.execute(sa.insert(Macroarea), params)


def insert_endangermentstatus(conn, *, bibitem_ids,
                              config_file='aes_status.ini'):
    log.info('insert endangermentstatus from %r:', config_file)
    status = Config.load(config_file, bind=conn)

    log.debug('insert %d endangermentstatus: %r', len(status), list(status))
    params = [{'name': s['name'].strip(),
               'config_section': section,
               'ordinal': int(s['ordinal']),
               'egids': s['egids'].strip(),
               'unesco': s['unesco'].strip(),
               'elcat': s['elcat'].strip(),
               'icon': s['icon'].strip(),
               'bibitem_id': bibitem_ids[s['reference_id'].strip()
                                         .partition(':')[::2]]}
              for section, s in status.items()]
    conn.execute(sa.insert(EndangermentStatus), params)


def insert_languoids(conn, *, languoids, bibitem_ids, es_ids):
    log.info('insert languoids')

    def unseen_countries(countries, _seen={}):
        for c in countries:
            id_, name = (c[k] for k in ('id', 'name'))
            try:
                assert _seen[id_] == name
            except KeyError:
                _seen[id_] = name
                yield c

    kwargs = {'conn': conn,
              'insert_lang': functools.partial(conn.execute, sa.insert(Languoid)),
              'unseen_countries': unseen_countries,
              'sourceprovider_ids': ModelMap(conn=conn, model=SourceProvider),
              'bibitem_ids': bibitem_ids,
              'altnameprovider_ids': ModelMap(conn=conn, model=AltnameProvider),
              'identifiersite_ids': ModelMap(conn=conn, model=IdentifierSite),
              'es_ids': es_ids}

    for _, l in languoids:
        insert_languoid(l, **kwargs)


def insert_languoid(languoid, *, conn,
                    insert_lang,
                    unseen_countries,
                    sourceprovider_ids,
                    bibitem_ids,
                    altnameprovider_ids,
                    identifiersite_ids,
                    es_ids):
    macroareas = languoid.pop('macroareas')
    countries = languoid.pop('countries')
    links = languoid.pop('links')
    timespan = languoid.pop('timespan', None)

    sources = languoid.pop('sources', None)
    altnames = languoid.pop('altnames', None)
    triggers = languoid.pop('triggers', None)
    identifier = languoid.pop('identifier', None)
    classification = languoid.pop('classification', None)
    endangerment = languoid.pop('endangerment', None)
    hh_ethnologue_comment = languoid.pop('hh_ethnologue_comment', None)
    iso_retirement = languoid.pop('iso_retirement', None)

    lid = languoid['id']
    insert_lang(languoid)

    if macroareas:
        conn.execute(sa.insert(languoid_macroarea),
                     [{'languoid_id': lid, 'macroarea_name': ma}
                      for ma in macroareas])

    if countries:
        new_countries = list(unseen_countries(countries))
        if new_countries:
            ids = [n['id'] for n in new_countries]
            log.debug('insert new countries: %r', ids)

            conn.execute(sa.insert(Country), new_countries)

        conn.execute(sa.insert(languoid_country),
                     [{'languoid_id': lid, 'country_id': c['id']}
                      for c in countries])

    if links:
        conn.execute(sa.insert(Link),
                     [dict(languoid_id=lid, ord=i, **link)
                      for i, link in enumerate(links, start=1)])

    if timespan:
        conn.execute(sa.insert(Timespan),
                     dict(languoid_id=lid, **timespan))

    if sources is not None:
        for provider, data in sources.items():
            provider_id = sourceprovider_ids[provider]
            conn.execute(sa.insert(Source),
                         [dict(languoid_id=lid,
                               provider_id=provider_id,
                               bibitem_id=bibitem_ids.pop_params(s), **s)
                          for s in data])

    if altnames is not None:
        for provider, names in altnames.items():
            provider_id = altnameprovider_ids[provider]
            half, full = groups = ([], [])
            for n in names:
                r = dict(languoid_id=lid, provider_id=provider_id, **n)
                if 'lang' not in r:
                    half.append(r)
                elif not r['lang']:  # lang = r.get('lang') or server_default
                    r.pop('lang')
                    half.append(r)
                else:
                    full.append(r)
            for rows in groups:
                if rows:
                    conn.execute(sa.insert(Altname), rows)

    if triggers is not None:
        conn.execute(sa.insert(Trigger),
                     [{'languoid_id': lid, 'field': field,
                       'trigger': t, 'ord': i}
                      for field, triggers in triggers.items()
                      for i, t in enumerate(triggers, start=1)])

    if identifier is not None:
        conn.execute(sa.insert(Identifier),
                     [dict(languoid_id=lid,
                           site_id=identifiersite_ids[site],
                           identifier=i)
                      for site, i in identifier.items()])

    if classification is not None:
        for c, value in classification.items():
            isref, kind = CLASSIFICATION[c]
            if isref:
                conn.execute(sa.insert(ClassificationRef),
                             [dict(languoid_id=lid, kind=kind,
                                   bibitem_id=bibitem_ids.pop_params(r),
                                   ord=i, **r)
                              for i, r in enumerate(value, start=1)])
            else:
                conn.execute(sa.insert(ClassificationComment),
                             {'languoid_id': lid, 'kind': kind,
                              'comment': value})

    if endangerment is not None:
        source = es_ids.params_to_key(endangerment.pop('source'))
        conn.execute(sa.insert(Endangerment),
                     dict(languoid_id=lid, source_id=es_ids[source],
                          **endangerment))

    if hh_ethnologue_comment is not None:
        conn.execute(sa.insert(EthnologueComment),
                     dict(languoid_id=lid, **hh_ethnologue_comment))

    if iso_retirement is not None:
        change_to = iso_retirement.pop('change_to')
        conn.execute(sa.insert(IsoRetirement),
                     dict(languoid_id=lid, **iso_retirement))

        if change_to:
            conn.execute(sa.insert(IsoRetirementChangeTo),
                         [{'languoid_id': lid, 'code': c, 'ord': i}
                          for i, c in enumerate(change_to, start=1)])
