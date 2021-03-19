# languoids.py - load languoids/tree/**/md.ini into dicts

import datetime
import itertools
import logging
import re
import warnings

import pycountry

from . import ENGINE, ROOT

from . import tools as _tools

__all__ = ['iterlanguoids',
           'compare_with_files', 'compare',
           'write_files']

FLOAT_DIGITS = 12

FLOAT_FORMAT = f'%.{FLOAT_DIGITS}f'

DATE_FORMAT = '%Y-%m-%d'

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

ISO_8601_INTERVAL = re.compile(r'(?P<start_sign>[+-]?)'
                               r'(?P<start_date>\d{1,4}-\d{2}-\d{2})'
                               r'/'
                               r'(?P<end_sign>[+-]?)'
                               r'(?P<end_date>\d{1,4}-\d{2}-\d{2})',
                               flags=re.ASCII)


log = logging.getLogger(__name__)


def iterlanguoids(root_or_bind=ROOT, *, from_raw=False, ordered=True,
                  progress_after=_tools.PROGRESS_AFTER):
    """Yield dicts from languoids/tree/**/md.ini files."""
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

        if ordered is True:  # insert languoids in id order if available
            ordered = 'id'

        iterfiles = raw.iterrecords(bind=bind,
                                    ordered=ordered,
                                    progress_after=progress_after)
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

    n = 0
    for n, (path_tuple, cfg) in enumerate(iterfiles, 1):
        languoid = make_languoid(path_tuple, cfg, from_raw=from_raw)
        yield path_tuple, languoid
    log.info('%s languoids extracted', f'{n:_d}')


def get_float(mapping, key, format_=FLOAT_FORMAT):
    result = mapping.get(key)
    if result is not None:
        result = float(format_ % float(result))
    return result


def format_float(value, format_=FLOAT_FORMAT):
    if value is None:
        return None
    return str(float(format_ % value))


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


def make_date(value, *, format_=DATE_FORMAT):
    return datetime.datetime.strptime(value, format_).date()


def format_date(value, *, format_=DATE_FORMAT):
    return value.strftime(format)


def make_datetime(value, *, format_=DATETIME_FORMAT):
    return datetime.datetime.strptime(value, format_)


def format_datetime(value, *, format_=DATETIME_FORMAT):
    return value.strftime(format_)


def make_interval(value, date_format=DATE_FORMAT, fix_year=True,
                  _match=ISO_8601_INTERVAL.fullmatch, strict=False):
    if value is None:
        return None
    value = value.strip()
    ma = _match(value)
    if ma is None:
        warnings.warn(f'unmatched interval: {value!r}')

        if strict:  # pragma: no cover
            log.error('invalid interval', value)
            raise ValueError('invalid interval', value)

        log.warning('ignoring interval value %r', value)
        return None

    dates = ma.group('start_date', 'end_date')
    if fix_year:
        def fix_date(d, year_tmpl='{:04d}'):
            year, sep, rest = d.partition('-')
            assert year and sep and rest
            year = year_tmpl.format(int(year))
            return f'{year}{sep}{rest}'

        dates = list(map(fix_date, dates))

    start, end = (datetime.datetime.strptime(d, date_format).date()
                  for d in dates)

    start_sign = -1 if ma.group('start_sign') == '-' else 1
    end_sign = -1 if ma.group('end_sign') == '-' else 1

    return {'start_year': start.year * start_sign,
            'start_month': start.month,
            'start_day': start.day,
            'end_year': end.year * end_sign,
            'end_month': end.month,
            'end_day': end.day}


def format_interval(value, year_tmpl='{: 05d}'):
    if value is None:
        return None

    # https://en.wikipedia.org/wiki/ISO_8601#Years
    assert -9999 <= value['start_year'] <= 9999
    assert -9999 <= value['end_year'] <= 9999

    context = dict(value,
                   start_year=year_tmpl.format(value.pop('start_year')).strip(),
                   end_year=year_tmpl.format(value.pop('end_year')).strip())

    return ('{start_year}-{start_month:02d}-{start_day:02d}'
            '/'
            '{end_year}-{end_month:02d}-{end_day:02d}').format_map(context)


def splitcountry(name, *, _match=re.compile(r'(?P<id_only>[A-Z]{2})'
                                            r'|'
                                            r'(?:'
                                                r'(?P<name>.+?)'
                                                r' '
                                                r'\('
                                                r'(?P<id>[^)]+)'
                                                r'\)'
                                            r')').fullmatch):
    groups = _match(name).groupdict()
    id_only = groups.pop('id_only')
    if id_only:
        country = pycountry.countries.get(alpha_2=id_only)
        return {'id': id_only, 'name': country.name}
    return groups


def formatcountry(value, minimal=True):
    return ('{name} ({id})' if not minimal else '{id}').format_map(value)


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


def formatlink(value):
    if value.get('title') is None:
        return value['url']
    return '[{title}]({url})'.format_map(value)


def splitsource(s, *, _match=re.compile(r'\*{2}'
                                        r'(?P<bibfile>[a-z0-9_\-]+)'
                                        r':'
                                        r"(?P<bibkey>[a-zA-Z0-9_\-/.;:?!'()\[\]]+?)"
                                        r'\*{2}'
                                        r'(?:'
                                            r':'
                                            r'(?P<pages>'
                                                r'[0-9]+(?:-[0-9]+)?'
                                                r'(?:[,;] [0-9]+(?:-[0-9]+)?)*'
                                           r')'
                                        r')?'
                                        r'(?:'
                                            r'<trigger "'
                                            r'(?P<trigger>[^\"]+)'
                                        r'">'
                                        r')?').match,  # <=v4.1 compat
                endangerment=False):
    if endangerment and s.isalnum():
        return {'name': s, 'bibfile': None, 'bibkey': None, 'pages': None}

    result = _match(s).groupdict()
    if endangerment:
        result['name'] = s
        result.pop('trigger', None)
    return result


def formatsource(value, endangerment=False):
    if endangerment and value.get('bibfile') is None:
        return value['name']

    result = ['**{bibfile}:{bibkey}**'.format_map(value)]
    if value.get('pages') is not None:
        result.append(':{pages}'.format_map(value))
    if value.get('trigger') is not None:
        result.append('<trigger "{trigger}">'.format_map(value))
    return ''.join(result)


def splitaltname(s, *, _match=re.compile(r'(?P<name>.+?)'
                                         r'(?: '
                                             r'\['
                                             r'(?P<lang>[a-z]{2,3})'
                                             r'\]'
                                         r')?').fullmatch):
    return _match(s).groupdict()


def formataltname(value):
    if value.get('lang') in ('', None):
        return value['name']
    return '{name} [{lang}]'.format_map(value)


def make_languoid(path_tuple, cfg, *, from_raw):
    _make_lines = make_lines_raw if from_raw else make_lines

    core = cfg['core']

    languoid = {'id': path_tuple[-1],
                'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
                'level': core['level'],
                'name': core['name'],
                'hid': core.get('hid'),
                'iso639_3': core.get('iso639-3'),
                'latitude': get_float(core, 'latitude'),
                'longitude': get_float(core, 'longitude'),
                'macroareas': _make_lines(core.get('macroareas')),
                'countries': [splitcountry(c)
                              for c in _make_lines(core.get('countries'))],
                'links': [splitlink(c) for c in _make_lines(core.get('links'))],
                'sources': None,
                'altnames': None,
                'triggers': None,
                'identifier': None,
                'classification': None,
                'endangerment': None,
                'hh_ethnologue_comment': None,
                'iso_retirement': None}

    # 'timespan' key is optional for backwards compat
    timespan = core.get('timespan')
    if timespan:
        languoid['timespan'] = make_interval(timespan)

    if 'sources' in cfg:
        sources = skip_empty({
            provider: [splitsource(p) for p in _make_lines(sources)]
            for provider, sources in cfg['sources'].items()
        })
        if sources:
            languoid['sources'] = sources

    if 'altnames' in cfg:
        altnames = {
            provider: [splitaltname(a) for a in _make_lines(altnames)]
            for provider, altnames in cfg['altnames'].items()
        }
        if altnames:
            languoid['altnames'] = altnames

    if 'triggers' in cfg:
        triggers = {
            field: _make_lines(triggers)
            for field, triggers in cfg['triggers'].items()
        }
        if triggers:
            languoid['triggers'] = triggers

    if 'identifier' in cfg:
        # FIXME: semicolon-separated (wals)?
        identifier = dict(cfg['identifier'])
        if identifier:
            languoid['identifier'] = identifier

    if 'classification' in cfg:
        classification = skip_empty({
            c: list(map(splitsource, _make_lines(classifications)))
               if c.endswith('refs') else
               classifications
            for c, classifications in cfg['classification'].items()
        })
        if classification:
            languoid['classification'] = classification

    if 'endangerment' in cfg:
        sct = cfg['endangerment']
        languoid['endangerment'] = {'status': sct['status'],
                                    'source': splitsource(sct['source'],
                                                          endangerment=True),
                                    'date': make_datetime(sct['date']),
                                    'comment': sct['comment']}

    if 'hh_ethnologue_comment' in cfg:
        sct = cfg['hh_ethnologue_comment']
        languoid['hh_ethnologue_comment'] = {'isohid': sct['isohid'],
                                             'comment_type': sct['comment_type'],
                                             'ethnologue_versions': sct['ethnologue_versions'],
                                             'comment': sct['comment']}

    if 'iso_retirement' in cfg:
        sct = cfg['iso_retirement']
        languoid['iso_retirement'] = {'code': sct['code'],
                                      'name': sct['name'],
                                      'change_request': sct.get('change_request'),
                                      'effective': make_date(sct['effective']),
                                      'reason': sct['reason'],
                                      'change_to': _make_lines(sct.get('change_to')),
                                      'remedy': sct.get('remedy'),
                                      'comment': sct.get('comment')}

    return languoid


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


def iterrecords(languoids):
    for p, l in languoids:
        rec = {'name': l['name'],
               'hid': l['hid'],
               'level': l['level'],
               'iso639-3': l['iso639_3'],
               'latitude': format_float(l['latitude']),
               'longitude': format_float(l['longitude']),
               'macroareas': l['macroareas'],
               'countries': list(map(formatcountry, l['countries'])),
               'links': list(map(formatlink, l['links'])),
               'timespan': format_interval(l.get('timespan'))}
        rec = {'core': rec}

        sources = l.get('sources') or {}
        if sources:
            sources = {p: list(map(formatsource, s)) for p, s in sources.items()}

        altnames = l.get('altnames') or {}
        if altnames:
            altnames = {p: list(map(formataltname, a)) for p, a in altnames.items()}

        triggers = l.get('triggers') or {}

        identifier = l.get('identifier') or {}

        classification = l['classification'] or {}
        if classification:
            classification.update({k: list(map(formatsource, classification[k]))
                                   for k in ('subrefs', 'familyrefs')
                                   if k in classification})

        endangerment = l.get('endangerment') or {}
        if endangerment:
            endangerment.update(source=formatsource(endangerment['source'],
                                                    endangerment=True),
                                date=format_datetime(endangerment['date']))

        hh_ethnologue_comment = l.get('hh_ethnologue_comment') or {}

        iso_retirement = l.get('iso_retirement') or {}
        if iso_retirement and False:  # FIXME
            iso_retirement['effective'] = format_date(iso_retirement['effective'])

        rec.update(sources=sources,
                   altnames=altnames,
                   triggers=triggers,
                   identifier=identifier,
                   classification=classification,
                   endangerment=endangerment,
                   hh_ethnologue_comment=hh_ethnologue_comment,
                   iso_retirement=iso_retirement)

        yield p, rec


def write_files(root=ROOT, *, from_raw=False, replace=False,
                progress_after=_tools.PROGRESS_AFTER, bind=ENGINE):
    log.info('write from tables to tree')

    from . import files

    languoids = iterlanguoids(bind, from_raw=from_raw, ordered='path')
    records = iterrecords(languoids)

    return files.write_files(records, root=root, replace=replace,
                             progress_after=progress_after)
