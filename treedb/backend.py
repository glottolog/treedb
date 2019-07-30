# backend.py - sqlite3 database engine

from __future__ import unicode_literals

import re
import csv
import time
import zipfile
import datetime
import warnings
import functools
import contextlib

from . import _compat

from ._compat import pathlib

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from . import tools as _tools

from . import FILE, ROOT, ENCODING

__all__ = [
    'ENGINE', 'Model', 'Dataset', 'Session',
    'load',
    'export',
    'write_csv', 'print_rows',
]

ENGINE = sa.create_engine('sqlite:///%s' % FILE, echo=False)


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


Model = sa.ext.declarative.declarative_base()


class Dataset(Model):
    """Git commit loaded into the database."""

    __tablename__ = '__dataset__'

    id = sa.Column(sa.Boolean, sa.CheckConstraint('id'),
                   primary_key=True, server_default=sa.true())
    title = sa.Column(sa.Text, sa.CheckConstraint("title != ''"), nullable=False)
    git_commit = sa.Column(sa.String(40), sa.CheckConstraint('length(git_commit) = 40'), nullable=False, unique=True)
    git_describe = sa.Column(sa.Text, sa.CheckConstraint("git_describe != ''"), nullable=False, unique=True)
    clean = sa.Column(sa.Boolean, nullable=False)
    exclude_raw = sa.Column(sa.Boolean, nullable=False)


Session = sa.orm.sessionmaker(bind=ENGINE)


def load(root=ROOT, engine=ENGINE, rebuild=False,
         exclude_raw=False, from_raw=False, force_delete=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return filename."""
    if exclude_raw and from_raw:
        raise RuntimeError('exclude_raw and from_raw cannot both be True')
    assert engine.url.drivername == 'sqlite'

    dbfile = pathlib.Path(engine.url.database)
    if dbfile.exists():
        try:
            found = sa.select([Dataset.exclude_raw], bind=engine).scalar()
        except Exception as e:
            warnings.warn('error reading __dataset__: %r' % e)
            if force_delete:
                rebuild = True
            else:
                raise

        if rebuild or found != exclude_raw:
            dbfile.unlink()
        else:
            return dbfile

    application_id = sum(ord(c) for c in Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    # import here to register models for create_all()
    if not exclude_raw:
        from . import raw
    from . import models

    start = time.time()
    with engine.begin() as conn:
        conn.execute('PRAGMA application_id = %d' % application_id)
        Model.metadata.create_all(bind=conn)

    get_stdout = functools.partial(_tools.check_output, cwd=str(root))
    dataset = {
        'title': 'Glottolog treedb',
        'git_commit': get_stdout(['git', 'rev-parse', 'HEAD']),
        'git_describe': get_stdout(['git', 'describe', '--tags', '--always']),
        # neither changes in index nor untracked files
        'clean': not get_stdout(['git', 'status', '--porcelain']),
        'exclude_raw': exclude_raw,
    }

    if not exclude_raw:
        with engine.begin() as conn:
            conn.execute('PRAGMA synchronous = OFF')
            conn.execute('PRAGMA journal_mode = MEMORY')
            raw._load(root,
                      conn.execution_options(compiled_cache={}))

    from . import languoids

    with engine.begin() as conn:
        conn.execute('PRAGMA synchronous = OFF')
        conn.execute('PRAGMA journal_mode = MEMORY')
        models._load(languoids.iterlanguoids(root=None if from_raw else root),
                     conn.execution_options(compiled_cache={}))

    sa.insert(Dataset, bind=engine).execute(dataset)

    print(datetime.timedelta(seconds=time.time() - start))
    return dbfile


def export(metadata=Model.metadata, engine=ENGINE, encoding=ENCODING):
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


def write_csv(query, filename, encoding=ENCODING, engine=ENGINE, verbose=False):
    if verbose:
        print(query)
    rows = engine.execute(query)
    with _compat.csv_open(filename, 'w', encoding) as f:
        writer = csv.writer(f)
        _compat.csv_write(writer, encoding, header=rows.keys(), rows=rows)
    return filename


def print_rows(query, format_=None, engine=ENGINE, verbose=False):
    if verbose:
        print(query)
    rows = engine.execute(query)
    if format_ is None:
        for r in rows:
            print(r)
    else:
        for r in rows:
            print(format_.format(**r))
