# insert raw model tables

import functools
import logging

import sqlalchemy as sa

from .. import _tools
from .. import fields as _fields
from .. import files as _files

from .models import File, Option, Value

__all__ = ['main']


log = logging.getLogger(__name__)


class OptionMap(dict):
    """Insert option on demand, add ``(section, option) -> (pk, is_lines)`` to map."""

    model = Option

    def __init__(self, items=(), *, conn):
        super().__init__(items)
        self.insert = functools.partial(conn.execute, sa.insert(self.model))

    def __missing__(self, key):
        log.debug('insert option: %r', key)
        section, option = key
        is_lines = _fields.is_lines(section, option)

        params = {'section': section, 'option': option,
                  'is_lines': is_lines}

        ord_section = _fields.SECTION_ORDER.get(section)
        if (ord_section is not None
            and _fields.is_all_options(*key)):
            ord_option = 0
            params.update(ord_option=ord_option,
                          defined=True,
                          defined_any_options=True)
        else:
            ord_option = _fields.FIELD_ORDER.get(key)
            params.update(ord_option=ord_option,
                          defined=ord_option is not None,
                          defined_any_options=False)

        params.update(ord_section=ord_section,
                      ord_option=ord_option)

        pk, = self.insert(params).inserted_primary_key

        self[key] = result = (pk, is_lines)
        return result


def itervalues(cfg, file_id, *, option_map):
    get_line = _tools.next_count(start=1)
    for section, sec in cfg.items():
        for option, value in sec.items():
            option_id, is_lines = option_map[section, option]
            if is_lines:
                for v in value.strip().splitlines():
                    yield {'file_id': file_id, 'option_id': option_id,
                           'line': get_line(), 'value': v}
            else:
                yield {'file_id': file_id, 'option_id': option_id,
                       'line': get_line(), 'value': value}


def main(root, *, conn):
    insert_file = functools.partial(conn.execute, sa.insert(File))

    option_id_is_lines = OptionMap(conn=conn)

    insert_value = functools.partial(conn.execute, sa.insert(Value))

    for path_tuple, cfg, dentry in _files.iterfiles(root):
        sha256 = _tools.sha256sum(dentry.path, raw=True)

        file_params = {'glottocode': path_tuple[-1],
                       'path': '/'.join(path_tuple),
                       'size': dentry.stat().st_size,
                       'sha256': sha256.hexdigest()}
        file_id, = insert_file(file_params).inserted_primary_key

        value_params = list(itervalues(cfg, file_id, option_map=option_id_is_lines))

        insert_value(value_params)
