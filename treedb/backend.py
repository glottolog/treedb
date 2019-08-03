# backend.py - sqlite3 database engine

from __future__ import unicode_literals
from __future__ import print_function

import re
import time
import zipfile
import datetime
import warnings
import functools
import contextlib

from ._compat import ENCODING

import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.declarative

from . import tools as _tools

from . import ENGINE, ROOT

__all__ = [
    'Model', 'Dataset',
    'Session',
    'load', 'export',
]


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
         exclude_raw=False, from_raw=None, force_delete=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return filename."""
    if exclude_raw and from_raw:
        raise RuntimeError('exclude_raw and from_raw cannot both be True')
    elif from_raw is None:
        from_raw = not exclude_raw

    assert engine.url.drivername == 'sqlite'
    if engine.file is not None and engine.file.exists():
        try:
            found = sa.select([Dataset.exclude_raw], bind=engine).scalar()
        except Exception as e:
            warnings.warn('error reading __dataset__: %r' % e)
            if force_delete:
                rebuild = True
            else:
                raise

        if rebuild or found != exclude_raw:
            warnings.warn('deleting present file: %r' % engine.file)
            engine.dispose()
            engine.file.unlink()
        else:
            return engine

    @contextlib.contextmanager
    def begin(bind=engine):
        with bind.begin() as conn:
            conn.execute('PRAGMA synchronous = OFF')
            conn.execute('PRAGMA journal_mode = MEMORY')
            yield conn.execution_options(compiled_cache={})

    # import here to register models for create_all()
    if not exclude_raw:
        from . import raw
    from . import models_load

    application_id = sum(ord(c) for c in Dataset.__tablename__)
    assert application_id == 1122 == 0x462

    start = time.time()
    with begin() as conn:
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
        with begin() as conn:
            raw.load(root, conn)

    from . import languoids

    with begin() as conn:
        pairs = languoids.iterlanguoids(conn if from_raw else root)
        models_load.load(pairs, conn)

    with begin() as conn:
        sa.insert(Dataset, bind=conn).execute(dataset)

    print(datetime.timedelta(seconds=time.time() - start))
    return engine


def export(engine=ENGINE, filename=None, encoding=ENCODING, metadata=Model.metadata):
    """Write all tables to <tablename>.csv in <databasename>.zip."""
    if filename is None:
        filename = engine.file_with_suffix('.zip').parts[-1]

    with engine.connect() as conn, zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as z:
        for table in metadata.sorted_tables:
            rows = table.select(bind=conn).execute()
            header = rows.keys()
            data = _tools.write_csv(None, rows, header, encoding)
            z.writestr('%s.csv' % table.name, data)

    return _tools.path_from_filename(filename)
