"""Parse a ``languoids/tree/**/md.ini`` record into a languoid and serialize it."""

from collections.abc import Iterable, Iterator
import datetime
import logging
import operator
import re
import warnings

import pycountry

from .. import _globals

from . import fields as _fields

__all__ = ['pipe']

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


def pipe(items, /, *, dump: bool,
         convert_lines: bool):
    codec = _dump if dump else _parse
    return codec(items, convert_lines=convert_lines)


def _parse(records: Iterable[_globals.RecordItem], /, *,
           convert_lines: bool) -> Iterator[_globals.LanguoidItem]:
    r"""Yield languoid items from given record ítems (from raw).

    >>> dict(pipe({('abin1243',):
    ...            {'core': {'name': 'Abinomn',
    ...                      'hid': 'bsa',
    ...                      'level': 'language',
    ...                      'iso639-3': 'bsa',
    ...                      'latitude': '-2.92281',
    ...                      'longitude': '138.891',
    ...                      'macroareas': '\nPapunesia',
    ...                      'countries': '\nID',
    ...                      'links': ('\n[Abinomn](http://endangeredlanguages.com/lang/1763)'
    ...                                '\nhttps://www.wikidata.org/entity/Q56648'
    ...                                '\nhttps://en.wikipedia.org/wiki/Abinomn_language')}}}.items(),
    ...           dump=False, convert_lines=True))  # doctest: +NORMALIZE_WHITESPACE
    {('abin1243',):
     {'id': 'abin1243',
      'parent_id': None,
      'name': 'Abinomn',
      'level': 'language',
      'hid': 'bsa',
      'iso639_3': 'bsa',
      'latitude': -2.92281,
      'longitude': 138.891,
      'macroareas': ['Papunesia'],
      'countries': [{'id': 'ID', 'name': 'Indonesia'}],
      'links': [{'url': 'http://endangeredlanguages.com/lang/1763', 'title': 'Abinomn', 'scheme': 'http'},
                {'url': 'https://www.wikidata.org/entity/Q56648', 'title': None, 'scheme': 'https'},
                {'url': 'https://en.wikipedia.org/wiki/Abinomn_language', 'title': None, 'scheme': 'https'}],
      'timespan': None,
      'sources': None,
      'altnames': None,
      'triggers': None,
      'identifier': None,
      'classification': None,
      'endangerment': None,
      'hh_ethnologue_comment': None,
      'iso_retirement': None}}

    >>> dict(pipe({('abin1243',):
    ...            {'core': {'name': 'Abinomn',
    ...                      'hid': 'bsa',
    ...                      'level': 'language',
    ...                      'iso639-3': 'bsa',
    ...                      'latitude': '-2.92281',
    ...                      'longitude': '138.891',
    ...                      'macroareas': ['Papunesia'],
    ...                      'countries': ['ID'],
    ...                      'links': ['[Abinomn](http://endangeredlanguages.com/lang/1763)',
    ...                                'https://www.wikidata.org/entity/Q56648',
    ...                                'https://en.wikipedia.org/wiki/Abinomn_language'],
    ...                      'timespan': None}}}.items(),
    ...           dump=False, convert_lines=False))  # doctest: +NORMALIZE_WHITESPACE
    {('abin1243',):
     {'id': 'abin1243',
      'parent_id': None,
      'name': 'Abinomn',
      'level': 'language',
      'hid': 'bsa',
      'iso639_3': 'bsa',
      'latitude': -2.92281,
      'longitude': 138.891,
      'macroareas': ['Papunesia'],
      'countries': [{'id': 'ID', 'name': 'Indonesia'}],
      'links': [{'url': 'http://endangeredlanguages.com/lang/1763', 'title': 'Abinomn', 'scheme': 'http'},
                {'url': 'https://www.wikidata.org/entity/Q56648', 'title': None, 'scheme': 'https'},
                {'url': 'https://en.wikipedia.org/wiki/Abinomn_language', 'title': None, 'scheme': 'https'}],
      'timespan': None,
      'sources': None,
      'altnames': None,
      'triggers': None,
      'identifier': None,
      'classification': None,
      'endangerment': None,
      'hh_ethnologue_comment': None,
      'iso_retirement': None}}
    """
    n = 0
    make_item = _globals.LanguoidItem
    for n, (path, cfg) in enumerate(records, start=1):
        languoid = make_languoid(path, cfg, convert_lines=convert_lines)
        yield make_item(path, languoid)
    log.info('%s languoids extracted from records', f'{n:_d}')


def _dump(languoids: Iterable[_globals.LanguoidItem], /, *,
          convert_lines: bool) -> Iterator[_globals.RecordItem]:
    r"""

    >>> dict(pipe({('abin1243',): {
    ...             'id': 'abin1243',
    ...             'parent_id': None,
    ...             'name': 'Abinomn',
    ...             'level': 'language',
    ...             'hid': 'bsa',
    ...             'iso639_3': 'bsa',
    ...             'latitude': -2.92281,
    ...             'longitude': 138.891,
    ...             'macroareas': ['Papunesia'],
    ...             'countries': [{'name': 'Indonesia', 'id': 'ID'}],
    ...             'links': [{'url': 'http://endangeredlanguages.com/lang/1763',
    ...                        'title': 'Abinomn', 'scheme': 'http'},
    ...                       {'url': 'https://www.wikidata.org/entity/Q56648',
    ...                         'title': None, 'scheme': 'https'},
    ...                       {'url': 'https://en.wikipedia.org/wiki/Abinomn_language',
    ...                        'title': None, 'scheme': 'https'}],
    ...             'timespan': None,
    ...             'classification': None,
    ...             }}.items(),
    ...           dump=True, convert_lines=False))  # doctest: +NORMALIZE_WHITESPACE
    {('abin1243',):
     {'core': {'name': 'Abinomn',
               'hid': 'bsa',
               'level': 'language',
               'iso639-3': 'bsa',
               'latitude': '-2.92281',
               'longitude': '138.891',
               'macroareas': ['Papunesia'],
               'countries': ['ID'],
               'links': ['[Abinomn](http://endangeredlanguages.com/lang/1763)',
                         'https://www.wikidata.org/entity/Q56648',
                         'https://en.wikipedia.org/wiki/Abinomn_language'],
                         'timespan': None},
      'sources': {},
      'altnames': {},
      'triggers': {},
      'identifier': {},
      'classification': {},
      'endangerment': {},
      'hh_ethnologue_comment': {},
      'iso_retirement': {}}}
    >>> dict(pipe({('abin1243',): {
    ...             'id': 'abin1243',
    ...             'parent_id': None,
    ...             'name': 'Abinomn',
    ...             'level': 'language',
    ...             'hid': 'bsa',
    ...             'iso639_3': 'bsa',
    ...             'latitude': -2.92281,
    ...             'longitude': 138.891,
    ...             'macroareas': ['Papunesia'],
    ...             'countries': [{'name': 'Indonesia', 'id': 'ID'}],
    ...             'links': [{'url': 'http://endangeredlanguages.com/lang/1763',
    ...                        'title': 'Abinomn', 'scheme': 'http'},
    ...                       {'url': 'https://www.wikidata.org/entity/Q56648',
    ...                         'title': None, 'scheme': 'https'},
    ...                       {'url': 'https://en.wikipedia.org/wiki/Abinomn_language',
    ...                        'title': None, 'scheme': 'https'}],
    ...             'timespan': None,
    ...             'classification': None,
    ...             }}.items(),
    ...           dump=True, convert_lines=True))  # doctest: +NORMALIZE_WHITESPACE
    {('abin1243',):
     {'core': {'name': 'Abinomn',
               'hid': 'bsa',
               'level': 'language',
               'iso639-3': 'bsa',
               'latitude': '-2.92281',
               'longitude': '138.891',
               'macroareas': '\nPapunesia',
               'countries': '\nID',
               'links': '\n[Abinomn](http://endangeredlanguages.com/lang/1763)\nhttps://www.wikidata.org/entity/Q56648\nhttps://en.wikipedia.org/wiki/Abinomn_language',
               'timespan': None},
      'sources': {},
      'altnames': {},
      'triggers': {},
      'identifier': {},
      'classification': {},
      'endangerment': {},
      'hh_ethnologue_comment': {},
      'iso_retirement': {}}}
    """  # noqa: E501
    for path, l in languoids:
        record = make_record(l, convert_lines=convert_lines)
        yield path, record


def make_languoid(path_tuple: _globals.PathType, cfg: _globals.RecordType, /, *,
                  convert_lines: bool) -> _globals.LanguoidType:
    _make_lines = _fields.parse_lines if convert_lines else make_lines_raw

    core = cfg[CORE]

    languoid = {'id': path_tuple[-1],
                'parent_id': path_tuple[-2] if len(path_tuple) > 1 else None,
                'name': core['name'],
                'level': core['level'],
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


def make_record(languoid: _globals.LanguoidType, /, *,
                convert_lines: bool,
                is_lines=_fields.is_lines) -> _globals.RecordType:
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
        if isinstance(iso_retirement['effective'], datetime.date):
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

    if convert_lines:
        _, record = _fields.join_lines_inplace((None, record))

    return record


def make_lines_raw(value, /):
    """No-op.

    >>> make_lines_raw(None)
    []

    >>> make_lines_raw(['spam', 'eggs'])
    ['spam', 'eggs']
    """
    if value is None:
        return []
    return value


def skip_empty(mapping, /):
    """

    >>> skip_empty({'spam': None, 'eggs': [], 'bacon': 'ham'})
    {'bacon': 'ham'}
    """
    return {k: v for k, v in mapping.items() if v}


def get_float(mapping, key, /, *, format_=FLOAT_FORMAT):
    """

    >>> get_float({'spam': '1.42'}, 'spam')
    1.42

    >>> assert get_float({}, 'spam') is None
    >>> assert get_float({'eggs': 42}, 'spam') is None
    """
    result = mapping.get(key)
    if result is not None:
        result = float(format_ % float(result))
    return result


def format_float(value, /, *, format_=FLOAT_FORMAT):
    """

    >>> format_float(1.42)
    '1.42'

    >>> assert format_float(None) is None
    """
    if value is None:
        return None
    return str(float(format_ % value))


def make_date(value, /, *, format_=DATE_FORMAT):
    """

    >>> make_date('2001-12-31')
    datetime.date(2001, 12, 31)
    """
    return datetime.datetime.strptime(value, format_).date()


def format_date(value, /, *, format_=DATE_FORMAT):
    """

    >>> format_date(datetime.date(2001, 12, 31))
    '2001-12-31'
    """
    return value.strftime(format_)


def make_datetime(value, /, *, format_=DATETIME_FORMAT):
    """

    >>> make_datetime('2001-12-31T23:59:59')
    datetime.datetime(2001, 12, 31, 23, 59, 59)
    """
    return datetime.datetime.strptime(value, format_)


def format_datetime(value, /, *, format_=DATETIME_FORMAT):
    """

    >>> format_datetime(datetime.datetime(2001, 12, 31, 23, 59, 59))
    '2001-12-31T23:59:59'
    """
    return value.strftime(format_)


def make_interval(value, /, *, date_format=DATE_FORMAT, fix_year=True,
                  _match=ISO_8601_INTERVAL.fullmatch, strict=False):
    """

    >>> make_interval('-9999-01-01/+9999-12-31')
    {'start_year': -9999, 'start_month': 1, 'start_day': 1, 'end_year': 9999, 'end_month': 12, 'end_day': 31}

    >>> assert make_interval(None) is None
    """
    if value is None:
        return None
    value = value.strip()
    if (ma := _match(value)) is None:
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


def format_interval(value, /, *, year_tmpl='{: 05d}'):
    """

    >>> format_interval({'start_year': -9999,
    ...                  'start_month': 1,
    ...                  'start_day': 1,
    ...                  'end_year': 9999,
    ...                  'end_month': 12,
    ...                  'end_day': 31})
    '-9999-01-01/9999-12-31'

    >>> assert format_interval(None) is None
    """
    if value is None:
        return None

    year_values = [value.pop(key) for key in ('start_year', 'end_year')]
    # https://en.wikipedia.org/wiki/ISO_8601#Years
    for year in year_values:
        assert -9999 <= year <= 9999

    start_year, end_year = (year_tmpl.format(y).strip() for y in year_values)
    context = dict(value, start_year=start_year, end_year=end_year)

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


def splitcountry(name, /, *, _match=_COUNTRY_PATTERN.fullmatch):
    """

    >>> splitcountry('The Union of the Comoros (KM)')
    {'name': 'The Union of the Comoros', 'id': 'KM'}
    """
    groups = _match(name).groupdict()
    id_only = groups.pop('id_only')
    if id_only:
        country = pycountry.countries.get(alpha_2=id_only)
        return {'id': id_only, 'name': country.name}
    return groups


def formatcountry(value, /, *, minimal=True):
    """

    >>> formatcountry({'name': 'The Kingdom of Norway', 'id': 'NO'})
    'NO'

    >>> formatcountry({'name': 'The Kingdom of Norway', 'id': 'NO'},
    ...               minimal=False)
    'The Kingdom of Norway (NO)'
    """
    return ('{name} ({id})' if not minimal else '{id}').format_map(value)


_LINK_PATTERN = re.compile(r'''
\[
    (?P<title>[^]]+)
\]
\(
    (?P<url>[^)]+)
\)
'''.strip(), flags=re.VERBOSE)


def splitlink(markdown, /, *, _match=_LINK_PATTERN.fullmatch):
    """

    >>> splitlink('https://www.example.com')
    {'url': 'https://www.example.com', 'title': None, 'scheme': 'https'}

    >>> splitlink('http://www.example.com')
    {'url': 'http://www.example.com', 'title': None, 'scheme': 'http'}

    >>> splitlink('[Example](https://www.example.com)')
    {'url': 'https://www.example.com', 'title': 'Example', 'scheme': 'https'}

    >>> splitlink('www.example.com')
    {'url': 'www.example.com', 'title': None, 'scheme': None}
    """
    if (ma := _match(markdown)) is not None:
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


def formatlink(value, /):
    """

    >>> formatlink({'url': 'https://example.com'})
    'https://example.com'

    >>> formatlink({'url': 'https://example.com', 'title': 'Example'})
    '[Example](https://example.com)'
    """
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


def splitsource(s, /, *, _match=_SOURCE_PATTERN.match,  # pre v4.1 compat
                endangerment=False):
    """

    >>> splitsource('**hh:42**')
    {'bibfile': 'hh', 'bibkey': '42', 'pages': None, 'trigger': None}

    >>> splitsource('**hh:42**:23-55')
    {'bibfile': 'hh', 'bibkey': '42', 'pages': '23-55', 'trigger': None}

    >>> splitsource('**hh:42**:23-55, 123-155')
    {'bibfile': 'hh', 'bibkey': '42', 'pages': '23-55, 123-155', 'trigger': None}

    >>> splitsource('**hh:42**:23-55; 123-155<trigger "spam">')
    {'bibfile': 'hh', 'bibkey': '42', 'pages': '23-55; 123-155', 'trigger': 'spam'}
    """
    if endangerment and s.isalnum():
        return {'name': s, 'bibfile': None, 'bibkey': None, 'pages': None}

    result = _match(s).groupdict()
    if endangerment:
        result['name'] = s
        result.pop('trigger', None)
    return result


def formatsource(value, /, *, endangerment=False):
    """

    >>> formatsource({'bibfile': 'hh', 'bibkey': '23'})
    '**hh:23**'

    >>> formatsource({'bibfile': 'hh', 'bibkey': '23', 'pages': '1-23'})
    '**hh:23**:1-23'

    >>> formatsource({'bibfile': 'hh', 'bibkey': '23',
    ...               'pages': '1-23', 'trigger': 'spam'})
    '**hh:23**:1-23<trigger "spam">'

    >>> formatsource({'name': 'HH'}, endangerment=True)
    'HH'
    """
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


def splitaltname(s, /, *, _match=_ALTNAME_PATTERN.fullmatch):
    """

    >>> splitaltname('Spam')
    {'name': 'Spam', 'lang': None}

    >>> splitaltname('Späm [de]')
    {'name': 'Späm', 'lang': 'de'}

    >>> splitaltname('Späm [deu]')
    {'name': 'Späm', 'lang': 'deu'}
    """
    return _match(s).groupdict()


def formataltname(value, /):
    """

    >>> formataltname({'name': 'Spam'})
    'Spam'

    >>> formataltname({'name': 'Spam', 'lang': None})
    'Spam'

    >>> formataltname({'name': 'Spam', 'lang': ''})
    'Spam'

    >>> formataltname({'name': 'Späm', 'lang': 'de'})
    'Späm [de]'

    >>> formataltname({'name': 'Späm', 'lang': 'deu'})
    'Späm [deu]'
    """
    if value.get('lang') in ('', None):
        return value['name']
    return '{name} [{lang}]'.format_map(value)
