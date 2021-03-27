# load languoids/tree/**/md.ini into dicts

import datetime
import logging
import operator
import re
import warnings
import typing

import pycountry

from . import _globals
from . import fields as _fields

__all__ = ['pipe',
           'parse', 'dump']

(CORE,
 SOURCES,
 ALTNAMES, TRIGGERS, IDENTIFIER,
 CLASSIFICATION,
 ENGANGERMENT, HH_ETHNOLOGUE_COMMENT, ISO_RETIREMENT) = _fields.SECTIONS

FLOAT_DIGITS = 12

FLOAT_FORMAT = f'%.{FLOAT_DIGITS}f'

DATE_FORMAT = '%Y-%m-%d'

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

ISO_8601_INTERVAL = re.compile(r'''
(?P<start_sign>[+-]?)
(?P<start_date>\d{1,4}-\d{2}-\d{2})
/
(?P<end_sign>[+-]?)
(?P<end_date>\d{1,4}-\d{2}-\d{2})
'''.strip(), flags=(re.ASCII | re.VERBOSE))


log = logging.getLogger(__name__)


def pipe(mode, items, *, from_raw: bool):
    codec = {'parse': parse, 'dump': dump}[mode]
    kwargs = {'from_raw': from_raw} if mode == 'parse' else {}
    return codec(items, **kwargs)


def parse(records: typing.Iterable[_globals.RecordItem],
          *, from_raw: bool) -> typing.Iterator[_globals.LanguoidItem]:
    n = 0
    make_item = _globals.LanguoidItem
    for n, (path, cfg) in enumerate(records, 1):
        languoid = make_languoid(path, cfg, from_raw=from_raw)
        yield make_item(path, languoid)
    log.info('%s languoids extracted from records', f'{n:_d}')


def dump(languoids: typing.Iterable[_globals.LanguoidItem],
         *, from_raw: bool) -> typing.Iterator[_globals.RecordItem]:
    if from_raw:  # pragma: no cover
        raise NotImplementedError
    for path, l in languoids:
        record = make_record(l)
        yield path, record


def make_languoid(path_tuple: _globals.PathType, cfg: _globals.RecordType,
                  *, from_raw: bool) -> _globals.LanguoidType:
    _make_lines = make_lines_raw if from_raw else make_lines

    core = cfg[CORE]

    languoid = {'id': path_tuple[-1],
                'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
                'level': core['level'],
                'name': core['name'],
                'hid': core.get('hid'),
                'iso639_3': core.get('iso639-3'),
                'latitude': get_float(core, 'latitude'),
                'longitude': get_float(core, 'longitude'),
                'macroareas': _make_lines(core.get('macroareas')),
                'countries': sorted((splitcountry(c)
                                     for c in _make_lines(core.get('countries'))),
                                    key=operator.itemgetter('id')),
                'links': [splitlink(c) for c in _make_lines(core.get('links'))],
                'timespan': make_interval(core.get('timespan')),
                'sources': None,
                'altnames': None,
                'triggers': None,
                'identifier': None,
                'classification': None,
                'endangerment': None,
                'hh_ethnologue_comment': None,
                'iso_retirement': None}

    if SOURCES in cfg:
        sources = {provider: [splitsource(p) for p in _make_lines(sources)]
                   for provider, sources in cfg['sources'].items()}
        sources = skip_empty(sources)
        if sources:
            languoid[SOURCES] = sources

    if ALTNAMES in cfg:
        altnames = {provider: [splitaltname(a) for a in _make_lines(altnames)]
                    for provider, altnames in cfg[ALTNAMES].items()}
        if altnames:
            languoid[ALTNAMES] = altnames

    if TRIGGERS in cfg:
        triggers = {field: _make_lines(triggers)
                    for field, triggers in cfg[TRIGGERS].items()}
        if triggers:
            languoid[TRIGGERS] = triggers

    if IDENTIFIER in cfg:
        # FIXME: semicolon-separated (wals)?
        identifier = dict(cfg[IDENTIFIER])
        if identifier:
            languoid[IDENTIFIER] = identifier

    if CLASSIFICATION in cfg:
        classification = {c: (list(map(splitsource, _make_lines(classifications)))
                              if c.endswith('refs') else classifications)
                          for c, classifications in cfg[CLASSIFICATION].items()}
        classification = skip_empty(classification)
        if classification:
            languoid[CLASSIFICATION] = classification

    if ENGANGERMENT in cfg:
        sct = cfg[ENGANGERMENT]
        languoid[ENGANGERMENT] = {'status': sct['status'],
                                  'source': splitsource(sct['source'],
                                                        endangerment=True),
                                  'date': make_datetime(sct['date']),
                                  'comment': sct['comment']}

    if HH_ETHNOLOGUE_COMMENT in cfg:
        sct = cfg[HH_ETHNOLOGUE_COMMENT]
        languoid[HH_ETHNOLOGUE_COMMENT] = {'isohid': sct['isohid'],
                                           'comment_type': sct['comment_type'],
                                           'ethnologue_versions': sct['ethnologue_versions'],
                                           'comment': sct['comment']}

    if ISO_RETIREMENT in cfg:
        sct = cfg[ISO_RETIREMENT]
        languoid[ISO_RETIREMENT] = {'code': sct['code'],
                                    'name': sct['name'],
                                    'change_request': sct.get('change_request'),
                                    'effective': make_date(sct['effective']),
                                    'reason': sct['reason'],
                                    'change_to': _make_lines(sct.get('change_to')),
                                    'remedy': sct.get('remedy'),
                                    'comment': sct.get('comment')}

    return languoid


def make_record(languoid: _globals.LanguoidType) -> _globals.RecordType:
    core = {'name': languoid['name'],
            'hid': languoid['hid'],
            'level': languoid['level'],
            'iso639-3': languoid['iso639_3'],
            'latitude': format_float(languoid['latitude']),
            'longitude': format_float(languoid['longitude']),
            'macroareas': languoid['macroareas'],
            'countries': list(map(formatcountry, languoid['countries'])),
            'links': list(map(formatlink, languoid['links'])),
            'timespan': format_interval(languoid.get('timespan'))}

    record = {CORE: core}

    sources = languoid.get('sources')
    if sources:
        sources = {p: list(map(formatsource, s)) for p, s in sources.items()}
    else:
        sources = {}

    altnames = languoid.get('altnames')
    if altnames:
        altnames = {p: list(map(formataltname, a)) for p, a in altnames.items()}
    else:
        altnames = {}

    triggers = languoid.get('triggers') or {}

    identifier = languoid.get('identifier') or {}

    classification = languoid['classification']
    if classification:
        classification.update({k: list(map(formatsource, classification[k]))
                               for k in ('subrefs', 'familyrefs')
                               if k in classification})
    else:
        classification = {}

    endangerment = languoid.get('endangerment')
    if endangerment:
        endangerment.update(source=formatsource(endangerment['source'],
                                                endangerment=True),
                            date=format_datetime(endangerment['date']))
    else:
        endangerment = {}

    hh_ethnologue_comment = languoid.get('hh_ethnologue_comment') or {}

    iso_retirement = languoid.get('iso_retirement')
    if iso_retirement:
        if False:  # FIXME
            iso_retirement['effective'] = format_date(iso_retirement['effective'])
    else:
        iso_retirement = {}

    record.update({SOURCES: sources,
                   ALTNAMES: altnames,
                   TRIGGERS: triggers,
                   IDENTIFIER: identifier,
                   CLASSIFICATION: classification,
                   ENGANGERMENT: endangerment,
                   HH_ETHNOLOGUE_COMMENT: hh_ethnologue_comment,
                   ISO_RETIREMENT: iso_retirement})

    return record


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


def get_float(mapping, key, format_=FLOAT_FORMAT):
    result = mapping.get(key)
    if result is not None:
        result = float(format_ % float(result))
    return result


def format_float(value, format_=FLOAT_FORMAT):
    if value is None:
        return None
    return str(float(format_ % value))


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


_COUNTRY_PATTERN = re.compile(r'''
(?P<id_only>[A-Z]{2})
|
(?:
    (?P<name>.+?)
    [ ]
    \(
        (?P<id>[^)]+)
    \)
)
'''.strip(), flags=re.VERBOSE)


def splitcountry(name, *, _match=_COUNTRY_PATTERN.fullmatch):
    groups = _match(name).groupdict()
    id_only = groups.pop('id_only')
    if id_only:
        country = pycountry.countries.get(alpha_2=id_only)
        return {'id': id_only, 'name': country.name}
    return groups


def formatcountry(value, minimal=True):
    return ('{name} ({id})' if not minimal else '{id}').format_map(value)


_LINK_PATTERN = re.compile(r'''
\[
    (?P<title>[^]]+)
\]
\(
    (?P<url>[^)]+)
\)
'''.strip(), flags=re.VERBOSE)


def splitlink(markdown, *, _match=_LINK_PATTERN.fullmatch):
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


_SOURCE_PATTERN = re.compile(r'''
\*{2}
    (?P<bibfile>[a-z0-9_\-]+)
    :
    (?P<bibkey>[a-zA-Z0-9_\-/.;:?!'()\[\]]+?)
\*{2}
(?:
    :
    (?P<pages>
        [0-9]+(?:-[0-9]+)?
        (?:[,;][ ][0-9]+(?:-[0-9]+)?)*
    )
)?
(?:
    <trigger[ ]"
        (?P<trigger>[^\"]+)
    ">
)?
'''.strip(), flags=re.VERBOSE)


def splitsource(s, *, _match=_SOURCE_PATTERN.match,  # pre v4.1 compat
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


_ALTNAME_PATTERN = re.compile(r'''
(?P<name>.+?)
(?:
    [ ]
     \[
         (?P<lang>[a-z]{2,3})
     \]
)?
'''.strip(), flags=re.VERBOSE)


def splitaltname(s, *, _match=_ALTNAME_PATTERN.fullmatch):
    return _match(s).groupdict()


def formataltname(value):
    if value.get('lang') in ('', None):
        return value['name']
    return '{name} [{lang}]'.format_map(value)
