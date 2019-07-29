# languoids.py - load ../../languoids/tree/**/md.ini into dicts

from __future__ import unicode_literals

import re
import datetime

from ._compat import iteritems

from . import ROOT

__all__ = ['iterlanguoids']


def get_type(mapping, key, type_):
    result = mapping.get(key)
    if result is None:
        return None
    return type_(result)


def make_lines(value):
    if value is None:
        return []
    return value.strip().splitlines()


def make_lines_raw(value):
    if value is None:
        return []
    return value


def skip_empty(mapping):
    return {k: v for k, v in iteritems(mapping) if v}


def make_date(value, format_='%Y-%m-%d'):
    return datetime.datetime.strptime(value, format_).date()


def make_datetime(value, format_='%Y-%m-%dT%H:%M:%S'):
    return datetime.datetime.strptime(value, format_)


def splitcountry(name, _match=re.compile(r'(.+) \(([^)]+)\)$').match):
    return _match(name).groups()


def splitlink(markdown, _match=re.compile(
    r'\[(?P<title>[^]]+)\]\((?P<url>[^)]+)\)$').match):
    ma = _match(markdown)
    if ma is not None:
        title, url = ma.groups()
    else:
        title = None
        url = markdown

    scheme, sep, rest = url.partition('://')
    if sep:
        assert rest
        scheme = scheme.lower()
    else:
        scheme = None
    return {'url': url, 'title': title, 'scheme': scheme}


def splitsource(s, _match=re.compile(
    r"\*\*(?P<bibfile>[a-z0-9\-_]+):(?P<bibkey>[a-zA-Z.?\-;*'/()\[\]!_:0-9\u2014]+?)\*\*"
    r"(:(?P<pages>[0-9\-f]+))?"
    r'(?:<trigger "(?P<trigger>[^\"]+)">)?').match):
    return _match(s).groupdict()


def splitaltname(s, _match=re.compile(
    r'(?P<name>[^[]+)'
    r'(?: \[(?P<lang>[a-z]{2,3})\])?$').match, parse_fail='!'):
    ma = _match(s)
    if ma is None:
        return {'name': s, 'lang': parse_fail}
    return ma.groupdict('')


def iterlanguoids(root=ROOT, from_raw=False):
    """Yield dicts from ../../languoids/tree/**/md.ini files."""
    if from_raw:
        from . import raw

        iterfiles = ((p.split('/'), r) for p, r in raw.iterrecords())
        _make_lines = make_lines_raw
    else:
        from . import files

        iterfiles = ((pt, cfg) for pt, _, cfg in files.iterconfig(root))
        _make_lines = make_lines

    for path_tuple, cfg in iterfiles:
        core = cfg['core']
        item = {
            'id': path_tuple[-1],
            'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
            'level': core['level'],
            'name': core['name'],
            'hid': core.get('hid'),
            'iso639_3': core.get('iso639-3'),
            'latitude': get_type(core, 'latitude', float),
            'longitude': get_type(core, 'longitude', float),
            'macroareas': _make_lines(core.get('macroareas')),
            'countries': [splitcountry(c) for c in _make_lines(core.get('countries'))],
            'links': [splitlink(c) for c in _make_lines(core.get('links'))],
        }

        if 'sources' in cfg:
            sources = skip_empty({
                provider: [splitsource(p) for p in _make_lines(sources)]
                for provider, sources in iteritems(cfg['sources'])
            })
            if sources:
                item['sources'] = sources

        if 'altnames' in cfg:
            item['altnames'] = {
                provider: [splitaltname(a) for a in _make_lines(altnames)]
                for provider, altnames in iteritems(cfg['altnames'])
            }

        if 'triggers' in cfg:
            item['triggers'] = {
                field: _make_lines(triggers)
                for field, triggers in iteritems(cfg['triggers'])
            }

        if 'identifier' in cfg:
            # FIXME: semicolon-separated (wals)?
            item['identifier'] = dict(cfg['identifier'])

        if 'classification' in cfg:
            classification = skip_empty({
                c: list(map(splitsource, _make_lines(classifications)))
                   if c.endswith('refs') else
                   classifications
                for c, classifications in iteritems(cfg['classification'])
            })
            if classification:
                item['classification'] = classification

        if 'endangerment' in cfg:
            sct = cfg['endangerment']
            item['endangerment'] = {
                'status': sct['status'],
                'source': sct['source'],
                'date': make_datetime(sct['date']),
                'comment': sct['comment'],
            }

        if 'hh_ethnologue_comment' in cfg:
            sct = cfg['hh_ethnologue_comment']
            item['hh_ethnologue_comment'] = {
                'isohid': sct['isohid'],
                'comment_type': sct['comment_type'],
                'ethnologue_versions': sct['ethnologue_versions'],
                'comment': sct['comment'],
            }

        if 'iso_retirement' in cfg:
            sct = cfg['iso_retirement']
            item['iso_retirement'] = {
                'code': sct['code'],
                'name': sct['name'],
                'change_request': sct.get('change_request'),
                'effective': make_date(sct['effective']),
                'reason': sct['reason'],
                'change_to': _make_lines(sct.get('change_to')),
                'remedy': sct.get('remedy'),
                'comment': sct.get('comment'),
            }

        yield item
