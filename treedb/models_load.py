# models_load.py

import logging

from ._compat import iteritems

from sqlalchemy import insert

from .models import (MACROAREA, CLASSIFICATION,
                     Languoid,
                     languoid_macroarea, Macroarea,
                     languoid_country, Country,
                     Link, Source, Altname, Trigger, Identifier,
                     ClassificationComment, ClassificationRef,
                     Endangerment, EthnologueComment,
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

    insert_link = insert(Link, bind=conn).execute
    insert_source = insert(Source, bind=conn).execute
    insert_altname = insert(Altname, bind=conn).execute
    insert_trigger = insert(Trigger, bind=conn).execute
    insert_ident = insert(Identifier, bind=conn).execute
    insert_comment = insert(ClassificationComment, bind=conn).execute
    insert_ref = insert(ClassificationRef, bind=conn).execute
    insert_enda = insert(Endangerment, bind=conn).execute
    insert_el = insert(EthnologueComment, bind=conn).execute

    insert_ir = insert(IsoRetirement, bind=conn).execute
    insert_irct = insert(IsoRetirementChangeTo, bind=conn).execute

    def unseen_countries(countries, _seen={}):
        for c in countries:
            try:
                assert _seen[c['id']] == c['name']
            except KeyError:
                _seen[c['id']] = c['name']
                yield c

    for _, l in languoids:
        lid = l['id']

        macroareas = l.pop('macroareas')
        countries = l.pop('countries')
        links = l.pop('links')

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

        if sources is not None:
            insert_source([dict(languoid_id=lid, provider=provider, ord=i, **s)
                           for provider, data in iteritems(sources)
                           for i, s in enumerate(data, 1)])

        if altnames is not None:
            insert_altname([dict(languoid_id=lid, provider=provider, ord=i, **n)
                            for provider, names in iteritems(altnames)
                            for i, n in enumerate(names, 1)])

        if triggers is not None:
            insert_trigger([{'languoid_id': lid, 'field': field,
                             'trigger': t, 'ord': i}
                            for field, triggers in iteritems(triggers)
                            for i, t in enumerate(triggers, 1)])

        if identifier is not None:
            insert_ident([dict(languoid_id=lid, site=site, identifier=i)
                          for site, i in iteritems(identifier)])

        if classification is not None:
            for c, value in iteritems(classification):
                isref, kind = CLASSIFICATION[c]
                if isref:
                    insert_ref([dict(languoid_id=lid, kind=kind, ord=i, **r)
                                for i, r in enumerate(value, 1)])
                else:
                    insert_comment(languoid_id=lid, kind=kind, comment=value)

        if endangerment is not None:
            insert_enda(languoid_id=lid, **endangerment)

        if hh_ethnologue_comment is not None:
            insert_el(languoid_id=lid, **hh_ethnologue_comment)

        if iso_retirement is not None:
            change_to = iso_retirement.pop('change_to')
            insert_ir(languoid_id=lid, **iso_retirement)

            if change_to:
                insert_irct([{'languoid_id': lid, 'code': c, 'ord': i}
                             for i, c in enumerate(change_to, 1)])
