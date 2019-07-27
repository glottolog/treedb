# backend.py - sqlite3 database engine

from __future__ import unicode_literals

import re
import csv
import time
import zipfile
import platform
import contextlib
import subprocess

from ._compat import pathlib

from . import ROOT

from . import _compat

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

__all__ = [
    'engine', 'Session', 'Model',
    'load', 'export',
    'write_csv', 'print_rows',
]

DBFILE = pathlib.Path('treedb.sqlite3')


engine = sa.create_engine('sqlite:///%s' % DBFILE, echo=False)


@sa.event.listens_for(sa.engine.Engine, 'connect')
def sqlite_engine_connect(dbapi_conn, connection_record):
    """Activate sqlite3 forein key checks, enable REGEXP operator."""
    with contextlib.closing(dbapi_conn.cursor()) as cursor:
        cursor.execute('PRAGMA foreign_keys = ON')
    dbapi_conn.create_function('regexp', 2, _regexp)


def _regexp(pattern, value):
    if value is None:
        return None
    return re.search(pattern, value) is not None


Session = sa.orm.sessionmaker(bind=engine)


Model = sa.ext.declarative.declarative_base()


class Dataset(Model):
    """Git commit loaded into the database."""

    __tablename__ = 'dataset'

    id = sa.Column(sa.Boolean, sa.CheckConstraint('id'),
                   primary_key=True, server_default=sa.true())
    git_commit = sa.Column(sa.String(40), sa.CheckConstraint('length(git_commit) = 40'), nullable=False, unique=True)
    git_describe = sa.Column(sa.Text, sa.CheckConstraint("git_describe != ''"), nullable=False, unique=True)
    clean = sa.Column(sa.Boolean, nullable=False)


def create_tables(bind=engine):
    Model.metadata.create_all(bind)


def load(load_func, rebuild=False, engine=engine):
    assert engine.url.drivername == 'sqlite'
    dbfile = pathlib.Path(engine.url.database)
    if dbfile.exists():
        if rebuild:
            dbfile.unlink()
        else:
            return dbfile

    def get_output(args, encoding='ascii', cwd=str(ROOT)):
        if platform.system() == 'Windows':
            STARTUPINFO = subprocess.STARTUPINFO()
            STARTUPINFO.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            STARTUPINFO.wShowWindow = subprocess.SW_HIDE
        else:
            STARTUPINFO = None
        stdout = subprocess.check_output(args, cwd=cwd, startupinfo=STARTUPINFO)
        return stdout.decode(encoding).strip()

    start = time.time()
    with engine.begin() as conn:
        create_tables(conn)
    infos = {
        'git_commit': get_output(['git', 'rev-parse', 'HEAD']),
        'git_describe': get_output(['git', 'describe', '--tags', '--always']),
        # neither changes in index nor untracked files
        'clean': not get_output(['git', 'status', '--porcelain']),
    }
    with engine.begin() as conn:
        conn.execute('PRAGMA synchronous = OFF')
        conn.execute('PRAGMA journal_mode = MEMORY')
        sa.insert(Dataset, bind=conn).execute(infos)
        load_func(conn.execution_options(compiled_cache={}))
    print(time.time() - start)
    return dbfile


def export(metadata=Model.metadata, engine=engine, encoding='utf-8'):
    """Write all tables to <tablename>.csv in <databasename>.zip."""
    filename = '%s.zip' % pathlib.Path(engine.url.database).stem
    with engine.connect() as conn, zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in metadata.sorted_tables:
            rows = table.select(bind=conn).execute()
            with _compat.make_csv_io() as f:
                writer = csv.writer(f)
                _compat.csv_write(writer, encoding, header=rows.keys(), rows=rows)
                data = f.getvalue()
            data = _compat.get_csv_io_bytes(data, encoding)
            z.writestr('%s.csv' % table.name, data)
    return filename


def write_csv(query, filename, encoding='utf-8', engine=engine, verbose=False):
    if verbose:
        print(query)
    rows = engine.execute(query)
    with _compat.csv_open(filename, 'w', encoding) as f:
        writer = csv.writer(f)
        _compat.csv_write(writer, encoding, header=rows.keys(), rows=rows)
    return filename


def print_rows(query, format_=None, engine=engine, verbose=False):
    if verbose:
        print(query)
    rows = engine.execute(query)
    if format_ is None:
        for r in rows:
            print(r)
    else:
        for r in rows:
            print(format_.format(**r))
