# languoids.py - load ../../languoids/tree/**/md.ini into dicts

from __future__ import unicode_literals

import re
import datetime

from . import files as _files

__all__ = ['iterlanguoids']


def getlines(cfg, section, option):
    if not cfg.has_option(section, option):
        return []
    return cfg.get(section, option).strip().splitlines()


def getdate(cfg, section, option, format_='%Y-%m-%d', **kwargs):
    value = cfg.get(section, option, **kwargs)
    if value is None:
        return None
    return datetime.datetime.strptime(value, format_).date()


def getdatetime(cfg, section, option, format_='%Y-%m-%dT%H:%M:%S'):
    return datetime.datetime.strptime(cfg.get(section, option), format_)


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
    scheme, sep, _ = url.partition('://')
    if sep:
        scheme = scheme.lower()
    else:
        scheme = None
    return {'url': url, 'title': title, 'scheme': scheme}


def splitsource(s, _match=re.compile(
    r"\*\*(?P<bibfile>[a-z0-9\-_]+):(?P<bibkey>[a-zA-Z.?\-;*'/()\[\]!_:0-9\u2014]+?)\*\*"
    r"(:(?P<pages>[0-9\-f]+))?"
    r'(<trigger "(?P<trigger>[^\"]+)">)?').match):
    return _match(s).groupdict()


def splitaltname(s, _match=re.compile(
    r'(?P<name>[^[]+)'
    r'(?: \[(?P<lang>[a-z]{2,3})\])?$').match, parse_fail='!'):
    ma = _match(s)
    if ma is None:
        return {'name': s, 'lang': parse_fail}
    return ma.groupdict('')


def iterlanguoids(root=None):
    """Yield dicts from ../../languoids/tree/**/md.ini files."""
    for path_tuple, _, cfg in _files.iterconfig(root):
        item = {
            'id': path_tuple[-1],
            'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
            'level': cfg.get('core', 'level'),
            'name': cfg.get('core', 'name'),
            'hid': cfg.get('core', 'hid', fallback=None),
            'iso639_3': cfg.get('core', 'iso639-3', fallback=None),
            'latitude': cfg.getfloat('core', 'latitude', fallback=None),
            'longitude': cfg.getfloat('core', 'longitude', fallback=None),
            'macroareas': getlines(cfg, 'core', 'macroareas'),
            'countries': [splitcountry(c) for c in getlines(cfg, 'core', 'countries')],
            'links': [splitlink(c) for c in getlines(cfg, 'core', 'links')],
        }
        if cfg.has_section('sources'):
            item['sources'] = {provider: [splitsource(p) for p in getlines(cfg, 'sources', provider)]
                               for provider in cfg.options('sources')}
        if cfg.has_section('altnames'):
            item['altnames'] = {provider: [splitaltname(a) for a in getlines(cfg, 'altnames', provider)]
                                for provider in cfg.options('altnames')}
        if cfg.has_section('triggers'):
            item['triggers'] = {field: getlines(cfg, 'triggers', field)
                                for field in cfg.options('triggers')}
        if cfg.has_section('identifier'):
            # FIXME: semicolon-separated (wals)?
            item['identifier'] = dict(cfg.items('identifier'))
        if cfg.has_section('classification'):
            item['classification'] = {
                c: list(map(splitsource, getlines(cfg, 'classification', c)))
                   if c.endswith('refs') else
                   cfg.get('classification', c)
                for c in cfg.options('classification')}
            assert item['classification']
        if cfg.has_section('endangerment'):
            item['endangerment'] = {
                'status': cfg.get('endangerment', 'status'),
                'source': cfg.get('endangerment', 'source'),
                'date': getdatetime(cfg, 'endangerment', 'date'),
                'comment': cfg.get('endangerment', 'comment'),
            }
        if cfg.has_section('hh_ethnologue_comment'):
            item['hh_ethnologue_comment'] = {
                'isohid': cfg.get('hh_ethnologue_comment', 'isohid'),
                'comment_type': cfg.get('hh_ethnologue_comment', 'comment_type'),
                'ethnologue_versions': cfg.get('hh_ethnologue_comment', 'ethnologue_versions'),
                'comment': cfg.get('hh_ethnologue_comment', 'comment'),
            }
        if cfg.has_section('iso_retirement'):
            item['iso_retirement'] = {
                'code': cfg.get('iso_retirement', 'code'),
                'name': cfg.get('iso_retirement', 'name'),
                'change_request': cfg.get('iso_retirement', 'change_request', fallback=None),
                'effective': getdate(cfg, 'iso_retirement', 'effective'),
                'reason': cfg.get('iso_retirement', 'reason'),
                'change_to': getlines(cfg, 'iso_retirement', 'change_to'),
                'remedy': cfg.get('iso_retirement', 'remedy', fallback=None),
                'comment': cfg.get('iso_retirement', 'comment', fallback=None),
            }
        yield item
