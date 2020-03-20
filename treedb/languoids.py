# languoids.py - load ../../languoids/tree/**/md.ini into dicts

import datetime
import functools
import itertools
import json
import logging
import operator
import re

from . import tools as _tools

from . import ROOT

__all__ = ['iterlanguoids', 'to_json_csv', 'compare_with_raw']


log = logging.getLogger(__name__)


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
    return {k: v for k, v in mapping.items() if v}


def make_date(value, format_='%Y-%m-%d'):
    return datetime.datetime.strptime(value, format_).date()


def make_datetime(value, format_='%Y-%m-%dT%H:%M:%S'):
    return datetime.datetime.strptime(value, format_)


def splitcountry(name, _match=re.compile(r'(?P<name>.+)'
                                         r' '
                                         r'\('
                                         r'(?P<id>[^)]+)'
                                         r'\)').fullmatch):
    return _match(name).groupdict()


def splitlink(markdown, _match=re.compile(r'\['
                                          r'(?P<title>[^]]+)'
                                          r'\]'
                                          r'\('
                                          r'(?P<url>[^)]+)'
                                          r'\)').fullmatch):
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


def splitsource(s, _match=re.compile(r'\*{2}'
                                     r'(?P<bibfile>[a-z0-9_\-]+)'
                                     r':'
                                     r"(?P<bibkey>[a-zA-Z0-9_\-/.;:?!*'()\[\]\u2014]+?)"
                                     r'\*{2}'
                                     r'(?:'
                                         r':'
                                         r'(?P<pages>'
                                             r'[0-9]+(?:-[0-9]+)?'
                                             r'(?:, [0-9]+(?:-[0-9]+)?)*'
                                         r')'
                                     r')?'
                                     r'(?:'
                                         r'<trigger "'
                                         r'(?P<trigger>[^\"]+)'
                                     r'">'
                                     r')?').match,
                endangerment=False):
    if endangerment and s.isalnum():
        return {'name': s}

    result = _match(s).groupdict()
    if endangerment:
        result.pop('trigger', None)
    return result


def splitaltname(s, _match=re.compile(r'(?P<name>[^[]+)'
                                      r'(?: '
                                          r'\['
                                          r'(?P<lang>[a-z]{2,3})'
                                          r'\]'
                                      r')?').fullmatch, parse_fail='!'):
    ma = _match(s)
    if ma is None:
        return {'name': s, 'lang': parse_fail}
    return ma.groupdict('')


def iterlanguoids(root_or_bind=ROOT):
    """Yield dicts from ../../languoids/tree/**/md.ini files."""
    log.info('extract languoids')

    if hasattr(root_or_bind, 'execute'):
        bind = root_or_bind

        from . import raw

        iterfiles = raw.iterrecords(bind)
        _make_lines = make_lines_raw
    else:
        root = root_or_bind

        from . import files

        iterfiles = ((pt, cfg) for pt, _, cfg in files.iterfiles(root))
        _make_lines = make_lines

    n = 0
    for n, (path_tuple, cfg) in enumerate(iterfiles, 1):
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
                for provider, sources in cfg['sources'].items()
            })
            if sources:
                item['sources'] = sources

        if 'altnames' in cfg:
            item['altnames'] = {
                provider: [splitaltname(a) for a in _make_lines(altnames)]
                for provider, altnames in cfg['altnames'].items()
            }

        if 'triggers' in cfg:
            item['triggers'] = {
                field: _make_lines(triggers)
                for field, triggers in cfg['triggers'].items()
            }

        if 'identifier' in cfg:
            # FIXME: semicolon-separated (wals)?
            item['identifier'] = dict(cfg['identifier'])

        if 'classification' in cfg:
            classification = skip_empty({
                c: list(map(splitsource, _make_lines(classifications)))
                   if c.endswith('refs') else
                   classifications
                for c, classifications in cfg['classification'].items()
            })
            if classification:
                item['classification'] = classification

        if 'endangerment' in cfg:
            sct = cfg['endangerment']
            item['endangerment'] = {
                'status': sct['status'],
                'source': splitsource(sct['source'], endangerment=True),
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

        yield path_tuple, item

    log.info('%d languoids extracted', n)


def to_json_csv(root_or_bind=ROOT, filename=None,
                dialect=_tools.DIALECT, encoding=_tools.ENCODING):
    """Write (path, json) rows for each languoid to filename."""
    if filename is None:
        suffix = '.languoids-json.csv'
        try:
            path = root_or_bind.file_with_suffix(suffix)
        except AttributeError:
            path = _tools.path_from_filename(root_or_bind).with_suffix(suffix)
        filename = path.name
    else:
        filename = _tools.path_from_filename(filename)

    log.info('write json csv: %r', filename)

    default_func = operator.methodcaller('isoformat')
    json_dumps = functools.partial(json.dumps, default=default_func)

    rows = (('/'.join(path_tuple), json_dumps(l))
            for path_tuple, l in iterlanguoids(root_or_bind))
    header = ['path', 'json']
    log.info('header: %r', header)

    return _tools.write_csv(filename, rows, header=header,
                            dialect=dialect, encoding=encoding)


def compare_with_raw(root=ROOT):
    from . import ENGINE

    same = True
    for files, raw in itertools.zip_longest(*map(iterlanguoids, (root, ENGINE))):
        if files != raw:
            same = False
            print('', '', files, '', raw, '', '', sep='\n')

    return same
