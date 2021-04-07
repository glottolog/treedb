# load glottolog languoid model tables

import functools
import logging

import sqlalchemy as sa

from . import _tools
from . import files
from .models import (CLASSIFICATION,
                     Config,
                     Languoid,
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

        self.insert = functools.partial(conn.execute, sa.insert(self.model))

    def __missing__(self, key):
        if self.log_insert:
            log.debug('insert new %s: %r', self.model.__tablename__, key)
        params = self.key_to_params(key)

        pk, = self.insert(params).inserted_primary_key

        self[key] = pk
        return pk


def main(languoids, *, conn):
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
              'altnameprovider_ids': ModelMap(conn=conn, model=AltnameProvider),
              'identifiersite_ids': ModelMap(conn=conn, model=IdentifierSite)}

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

    bibitem_ids = BibitemMap(conn=conn, log_insert=False)  # silence bibitems

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

    log.info('insert _config')
    insert_configs(conn)

    log.info('insert macroareas')
    insert_macroareas(conn)

    log.info('insert endangermentstatus')
    insert_endangermentstatus(conn, bibitem_ids=bibitem_ids)

    es_ids = EndangermentSourceMap(conn=conn)

    kwargs.update(bibitem_ids=bibitem_ids,
                  es_ids=es_ids)

    log.info('insert languoids')
    for _, l in languoids:
        insert_languoid(l, **kwargs)


def insert_configs(conn):
    for filename, cfg in files.iterconfigs():
        get_line = _tools.next_count(start=1)
        params = [{'filename': filename, 'section': section, 'option': option,
                   'value': value.strip(), 'line': get_line()}
                  for section, sec in cfg.items()
                  for option, value in sec.items()
                  if value.strip()]
        conn.execute(sa.insert(Config), params)


def load_config(conn, *, filename: str,
                _groupby_section=_tools.groupby_itemgetter(0)):
    select_values = (sa.select(Config.section, Config.option, Config.value)
                     .filter_by(filename=filename)
                     .order_by('section', 'option'))
    result = conn.execute(select_values)
    return {section: {option: value for _, option, value in grp}
            for section, grp in _groupby_section(result)}


def insert_macroareas(conn, *, config_file='macroareas.ini'):
    macroareas = load_config(conn, filename=config_file)
    log.debug('insert %d macroareas: %r', len(macroareas), list(macroareas))
    params = [{'name': m['name'], 'key': section, 'description': m['description']}
              for section, m in macroareas.items()]
    conn.execute(sa.insert(Macroarea), params)


def insert_endangermentstatus(conn, *, bibitem_ids,
                              config_file='aes_status.ini'):
    status = load_config(conn, filename=config_file)
    log.debug('insert %d endangermentstatus: %r', len(status), list(status))

    params = [{'name': s['name'], 'key': section,
               'ordinal': int(s['ordinal']),
               'egids': s['egids'],
               'unesco': s['unesco'],
               'elcat': s['elcat'],
               'icon': s['icon'],
               'bibitem_id': bibitem_ids[s['reference_id'].partition(':')[::2]]}
              for section, s in status.items()]
    conn.execute(sa.insert(EndangermentStatus), params)


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
