# languoids.py - load ../../languoids/tree/**/md.ini into dicts

import datetime
import itertools
import logging
import re

from . import tools as _tools

from . import ROOT, ENGINE

__all__ = ['iterlanguoids',
           'compare_with_files', 'compare']


log = logging.getLogger(__name__)


def get_float(mapping, key, format_='%.8f'):
    result = mapping.get(key)
    if result is not None:
        result = float(format_ % float(result))
    return result


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


def make_date(value, *, format_='%Y-%m-%d'):
    return datetime.datetime.strptime(value, format_).date()


def make_datetime(value, *, format_='%Y-%m-%dT%H:%M:%S'):
    return datetime.datetime.strptime(value, format_)


def splitcountry(name, *, _match=re.compile(r'(?P<name>.+?)'
                                            r' '
                                            r'\('
                                            r'(?P<id>[^)]+)'
                                            r'\)').fullmatch):
    return _match(name).groupdict()


def splitlink(markdown, *, _match=re.compile(r'\['
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


def splitsource(s, *, _match=re.compile(r'\*{2}'
                                        r'(?P<bibfile>[a-z0-9_\-]+)'
                                        r':'
                                        r"(?P<bibkey>[a-zA-Z0-9_\-/.;:?!'()\[\]]+?)"
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
    if endangerment:
        if s.isalnum():
            return {'name': s, 'bibfile': None, 'bibkey': None, 'pages': None}

    result = _match(s).groupdict()
    if endangerment:
        result['name'] = s
        result.pop('trigger', None)
    return result


def splitaltname(s, *, _match=re.compile(r'(?P<name>.+?)'
                                         r'(?: '
                                             r'\['
                                             r'(?P<lang>[a-z]{2,3})'
                                             r'\]'
                                         r')?').fullmatch):
    return _match(s).groupdict('')


def iterlanguoids(root_or_bind=ROOT, *, from_raw=False, ordered=True,
                  progress_after=_tools.PROGRESS_AFTER):
    """Yield dicts from ../../languoids/tree/**/md.ini files."""
    log.info('generate languoids')

    if hasattr(root_or_bind, 'execute'):
        bind = root_or_bind

        if not from_raw:
            from . import languoids_json

            yield from languoids_json.iterlanguoids(bind,
                                                    ordered=ordered,
                                                    progress_after=progress_after)
            return

        log.info('extract languoids from raw records')

        from . import raw

        iterfiles = raw.iterrecords(bind=bind,
                                    ordered=ordered,
                                    progress_after=progress_after)

        _make_lines = make_lines_raw

    else:
        log.info('extract languoids from files')
        root = root_or_bind

        if from_raw:
            raise TypeError(f'from_raw=True requires bind (passed: {root!r})')

        if ordered not in (True, False, 'file', 'path'):
            raise ValueError(f'ordered={ordered!r} not implemented')

        from . import files

        iterfiles = files.iterfiles(root, progress_after=progress_after)
        iterfiles = ((pt, cfg) for pt, _, cfg in iterfiles)

        _make_lines = make_lines

    n = 0
    for n, (path_tuple, cfg) in enumerate(iterfiles, 1):
        sources = None
        if 'sources' in cfg:
            sources = skip_empty({
                provider: [splitsource(p) for p in _make_lines(sources)]
                for provider, sources in cfg['sources'].items()
            }) or None

        altnames = None
        if 'altnames' in cfg:
            altnames = {
                provider: [splitaltname(a) for a in _make_lines(altnames)]
                for provider, altnames in cfg['altnames'].items()
            } or None

        triggers = None
        if 'triggers' in cfg:
            triggers = {
                field: _make_lines(triggers)
                for field, triggers in cfg['triggers'].items()
            } or None

        identifier = None
        if 'identifier' in cfg:
            # FIXME: semicolon-separated (wals)?
            identifier = dict(cfg['identifier']) or None

        classification = None
        if 'classification' in cfg:
            classification = skip_empty({
                c: list(map(splitsource, _make_lines(classifications)))
                   if c.endswith('refs') else
                   classifications
                for c, classifications in cfg['classification'].items()
            }) or None

        endangerment = None
        if 'endangerment' in cfg:
            sct = cfg['endangerment']
            endangerment = {
                'status': sct['status'],
                'source': splitsource(sct['source'], endangerment=True),
                'date': make_datetime(sct['date']),
                'comment': sct['comment'],
            }

        hh_ethnologue_comment = None
        if 'hh_ethnologue_comment' in cfg:
            sct = cfg['hh_ethnologue_comment']
            hh_ethnologue_comment = {
                'isohid': sct['isohid'],
                'comment_type': sct['comment_type'],
                'ethnologue_versions': sct['ethnologue_versions'],
                'comment': sct['comment'],
            }

        iso_retirement = None
        if 'iso_retirement' in cfg:
            sct = cfg['iso_retirement']
            iso_retirement = {
                'code': sct['code'],
                'name': sct['name'],
                'change_request': sct.get('change_request'),
                'effective': make_date(sct['effective']),
                'reason': sct['reason'],
                'change_to': _make_lines(sct.get('change_to')),
                'remedy': sct.get('remedy'),
                'comment': sct.get('comment'),
            }

        core = cfg['core']

        item = {
            'id': path_tuple[-1],
            'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
            'level': core['level'],
            'name': core['name'],
            'hid': core.get('hid'),
            'iso639_3': core.get('iso639-3'),
            'latitude': get_float(core, 'latitude'),
            'longitude': get_float(core, 'longitude'),
            'macroareas': _make_lines(core.get('macroareas')),
            'countries': [splitcountry(c) for c in _make_lines(core.get('countries'))],
            'links': [splitlink(c) for c in _make_lines(core.get('links'))],
            'sources': sources,
            'altnames': altnames,
            'triggers': triggers,
            'identifier': identifier,
            'classification': classification,
            'endangerment': endangerment,
            'hh_ethnologue_comment': hh_ethnologue_comment,
            'iso_retirement': iso_retirement,
        }

        yield path_tuple, item

    log.info('%s languoids extracted', f'{n:_d}')


def compare_with_files(bind=ENGINE, *, root=ROOT, from_raw=True):
    return compare(iterlanguoids(root, ordered=True),
                   iterlanguoids(bind, from_raw=from_raw, ordered='path'))


def compare(left, right):
    same = True
    for l, r in itertools.zip_longest(left, right):
        if l != r:
            same = False
            print('', '', l, '', r, '', '', sep='\n')

    return same
