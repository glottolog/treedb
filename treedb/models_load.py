# models_load.py

import logging

from sqlalchemy import insert

from .models import (MACROAREA, CLASSIFICATION,
                     Languoid,
                     languoid_macroarea, Macroarea,
                     languoid_country, Country,
                     Link, Timespan, Source, Bibfile, Bibitem,
                     Altname, Trigger, Identifier,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EndangermentSource,
                     EthnologueComment,
                     IsoRetirement, IsoRetirementChangeTo)

__all__ = ['load']


log = logging.getLogger(__name__)


def load(languoids, conn):
    insert_lang = insert(Languoid, bind=conn).execute

    macroareas = sorted(MACROAREA)
    log.debug('insert macroareas: %r', macroareas)
    insert(Macroarea, bind=conn).execute([{'name': n} for n in macroareas])

    lang_ma = languoid_macroarea.insert(bind=conn).execute

    insert_country = insert(Country, bind=conn).execute
    lang_country = languoid_country.insert(bind=conn).execute

    def unseen_countries(countries, _seen={}):
        for c in countries:
            id_, name = (c[k] for k in ('id', 'name'))
            try:
                assert _seen[id_] == name
            except KeyError:
                _seen[id_] = name
                yield c

    insert_link = insert(Link, bind=conn).execute

    insert_timespan = insert(Timespan, bind=conn).execute

    insert_source = insert(Source, bind=conn).execute
    insert_bibfile = insert(Bibfile, bind=conn).execute
    insert_bibitem = insert(Bibitem, bind=conn).execute

    class BibfileMap(dict):

        def __missing__(self, name):
            log.debug('insert new bibfile: %r', name)
            id, = insert_bibfile(name=name).inserted_primary_key
            self[name] = id
            return id

    bibfile_ids = BibfileMap()

    class BibitemMap(dict):

        def __missing__(self, key):
            bibfile_name, bibkey = key
            id, = insert_bibitem(bibfile_id=bibfile_ids[bibfile_name],
                                 bibkey=bibkey).inserted_primary_key
            self[key] = id
            return id

        def pop_params(self, params):
            return self[params.pop('bibfile'), params.pop('bibkey')]

    bibitem_ids = BibitemMap()

    insert_altname = insert(Altname, bind=conn).execute
    insert_trigger = insert(Trigger, bind=conn).execute
    insert_ident = insert(Identifier, bind=conn).execute
    insert_comment = insert(ClassificationComment, bind=conn).execute
    insert_ref = insert(ClassificationRef, bind=conn).execute

    insert_enda = insert(Endangerment, bind=conn).execute
    insert_enda_source = insert(EndangermentSource, bind=conn).execute

    class EndangermentSourceMap(dict):

        @staticmethod
        def params_to_key(params):
            return tuple(sorted(params.items()))

        def __missing__(self, key):
            params = dict(key)
            if params.get('bibfile') is not None:
                params['bibitem_id'] = bibitem_ids.pop_params(params)
            log.debug('insert new endangerment_source: %r', params)
            id, = insert_enda_source(**params).inserted_primary_key
            self[key] = id
            return id

    es_ids = EndangermentSourceMap()

    insert_el = insert(EthnologueComment, bind=conn).execute

    insert_ir = insert(IsoRetirement, bind=conn).execute
    insert_irct = insert(IsoRetirementChangeTo, bind=conn).execute

    for _, l in languoids:
        lid = l['id']

        macroareas = l.pop('macroareas')
        countries = l.pop('countries')
        links = l.pop('links')
        timespan = l.pop('timespan')

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
            lang_ma([{'languoid_id': lid, 'macroarea_name': ma}
                     for ma in macroareas])

        if countries:
            new_countries = list(unseen_countries(countries))
            if new_countries:
                ids = [n['id'] for n in new_countries]
                log.debug('insert new countries: %r', ids)

                insert_country(new_countries)

            lang_country([{'languoid_id': lid, 'country_id': c['id']}
                          for c in countries])

        if links:
            insert_link([dict(languoid_id=lid, ord=i, **link)
                         for i, link in enumerate(links, 1)])

        if timespan:
            insert_timespan(languoid_id=lid, **timespan)

        if sources is not None:
            insert_source([dict(languoid_id=lid, provider=provider,
                                bibitem_id=bibitem_ids.pop_params(s), **s)
                           for provider, data in sources.items()
                           for s in data])

        if altnames is not None:
            insert_altname([dict(languoid_id=lid, provider=provider, **n)
                            for provider, names in altnames.items()
                            for n in names])

        if triggers is not None:
            insert_trigger([{'languoid_id': lid, 'field': field,
                             'trigger': t, 'ord': i}
                            for field, triggers in triggers.items()
                            for i, t in enumerate(triggers, 1)])

        if identifier is not None:
            insert_ident([dict(languoid_id=lid, site=site, identifier=i)
                          for site, i in identifier.items()])

        if classification is not None:
            for c, value in classification.items():
                isref, kind = CLASSIFICATION[c]
                if isref:
                    insert_ref([dict(languoid_id=lid, kind=kind,
                                     bibitem_id=bibitem_ids.pop_params(r),
                                     ord=i, **r)
                                for i, r in enumerate(value, 1)])
                else:
                    insert_comment(languoid_id=lid, kind=kind, comment=value)

        if endangerment is not None:
            source = es_ids.params_to_key(endangerment.pop('source'))
            insert_enda(languoid_id=lid,
                        source_id=es_ids[source],
                        **endangerment)

        if hh_ethnologue_comment is not None:
            insert_el(languoid_id=lid, **hh_ethnologue_comment)

        if iso_retirement is not None:
            change_to = iso_retirement.pop('change_to')
            insert_ir(languoid_id=lid, **iso_retirement)

            if change_to:
                insert_irct([{'languoid_id': lid, 'code': c, 'ord': i}
                             for i, c in enumerate(change_to, 1)])
