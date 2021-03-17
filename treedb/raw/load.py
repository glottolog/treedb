# load.py

import functools
import logging

import sqlalchemy as sa

from .. import fields as _fields
from .. import files as _files
from .. import tools as _tools
from .models import File, Option, Value

__all__ = ['load']


log = logging.getLogger(__name__)


class Options(dict):
    """Insert optons on demand and cache id and lines config."""

    def __init__(self, items=(), *, conn, model):
        super().__init__(items)
        self.insert = functools.partial(conn.execute, sa.insert(model))

    def __missing__(self, key):
        log.debug('insert option %r', key)

        section, option = key
        is_lines = _fields.is_lines(section, option)

        params = {'section': section, 'option': option,
                  'is_lines': is_lines}
        id_, = self.insert(params).inserted_primary_key

        self[key] = result = (id_, is_lines)
        return result


def itervalues(cfg, file_id, options):
    get_line = _tools.next_count(start=1)
    for section, sec in cfg.items():
        for option, value in sec.items():
            option_id, is_lines = options[section, option]
            if is_lines:
                for v in value.strip().splitlines():
                    yield {'file_id': file_id, 'option_id': option_id,
                           'line': get_line(), 'value': v}
            else:
                yield {'file_id': file_id, 'option_id': option_id,
                       'line': get_line(), 'value': value}


def load(root, conn):
    insert_file = functools.partial(conn.execute, sa.insert(File))
    options = Options(conn=conn, model=Option)
    insert_value = functools.partial(conn.execute, sa.insert(Value))
    for path_tuple, dentry, cfg in _files.iterfiles(root):
        sha256 = _tools.sha256sum(dentry.path, raw=True).hexdigest()
        file_params = {'glottocode': path_tuple[-1],
                       'path': '/'.join(path_tuple),
                       'size': dentry.stat().st_size,
                       'sha256': sha256}
        file_id, = insert_file(file_params).inserted_primary_key

        value_params = list(itervalues(cfg, file_id, options))

        insert_value(value_params)
