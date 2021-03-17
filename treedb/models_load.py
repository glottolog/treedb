# models_load.py

import functools
import logging

import sqlalchemy as sa

from .models import (MACROAREA, CLASSIFICATION,
                     Languoid,
                     languoid_macroarea, Macroarea,
                     languoid_country, Country,
                     Link, Timespan,
                     Source, SourceProvider,
                     Bibfile, Bibitem,
                     Altname, AltnameProvider,
                     Trigger, Identifier, IdentifierSite,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['load']


log = logging.getLogger(__name__)


def load(languoids, conn):
    insert_lang = functools.partial(conn.execute, sa.insert(Languoid))

    macroareas = sorted(MACROAREA)
    log.debug('insert macroareas: %r', macroareas)
    conn.execute(sa.insert(Macroarea), [{'name': n} for n in macroareas])

    def unseen_countries(countries, _seen={}):
        for c in countries:
            id_, name = (c[k] for k in ('id', 'name'))
            try:
                assert _seen[id_] == name
            except KeyError:
                _seen[id_] = name
                yield c

    class SourceProviderMap(dict):

        insert = functools.partial(conn.execute, sa.insert(SourceProvider))

        def __missing__(self, name):
            log.debug('insert new source provider: %r', name)
            id, = self.insert({'name': name}).inserted_primary_key
            self[name] = id
            return id

    sourceprovider_ids = SourceProviderMap()

    class BibfileMap(dict):

        insert = functools.partial(conn.execute, sa.insert(Bibfile))

        def __missing__(self, name):
            log.debug('insert new bibfile: %r', name)
            id, = self.insert({'name': name}).inserted_primary_key
            self[name] = id
            return id

    bibfile_ids = BibfileMap()

    class BibitemMap(dict):

        insert = functools.partial(conn.execute, sa.insert(Bibitem))

        def __missing__(self, key):
            bibfile_name, bibkey = key
            id, = self.insert({'bibfile_id': bibfile_ids[bibfile_name],
                               'bibkey': bibkey}).inserted_primary_key
            self[key] = id
            return id

        def pop_params(self, params):
            return self[params.pop('bibfile'), params.pop('bibkey')]

    bibitem_ids = BibitemMap()

    class AltnameProviderMap(dict):

        insert = functools.partial(conn.execute, sa.insert(AltnameProvider))

        def __missing__(self, name):
            log.debug('insert new altname provider: %r', name)
            id, = self.insert({'name': name}).inserted_primary_key
            self[name] = id
            return id

    altnameprovider_ids = AltnameProviderMap()

    class IdentifierSiteMap(dict):

        insert = functools.partial(conn.execute, sa.insert(IdentifierSite))

        def __missing__(self, name):
            log.debug('insert new identifier site: %r', name)
            id, = self.insert({'name': name}).inserted_primary_key
            self[name] = id
            return id

    identifiersite_ids = IdentifierSiteMap()

    class EndangermentSourceMap(dict):

        insert = functools.partial(conn.execute, sa.insert(EndangermentSource))

        @staticmethod
        def params_to_key(params):
            return tuple(sorted(params.items()))

        def __missing__(self, key):
            params = dict(key)
            if params.get('bibfile') is not None:
                params['bibitem_id'] = bibitem_ids.pop_params(params)
            log.debug('insert new endangerment_source: %r', params)
            id, = self.insert(params).inserted_primary_key
            self[key] = id
            return id

    es_ids = EndangermentSourceMap()

    for _, l in languoids:
        lid = l['id']

        macroareas = l.pop('macroareas')
        countries = l.pop('countries')
        links = l.pop('links')
        timespan = l.pop('timespan', None)

        sources = l.pop('sources', None)
        altnames = l.pop('altnames', None)
        triggers = l.pop('triggers', None)
        identifier = l.pop('identifier', None)
        classification = l.pop('classification', None)
        endangerment = l.pop('endangerment', None)
        hh_ethnologue_comment = l.pop('hh_ethnologue_comment', None)
        iso_retirement = l.pop('iso_retirement', None)

        insert_lang(l)

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
                          for i, link in enumerate(links, 1)])

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
                          for i, t in enumerate(triggers, 1)])

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
                                  for i, r in enumerate(value, 1)])
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
                              for i, c in enumerate(change_to, 1)])
