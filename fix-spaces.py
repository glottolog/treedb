#!/usr/bin/env python3
# flake8: noqa

"""Fix double whitespace in classification > sub."""

import sqlalchemy as sa
import treedb


treedb.configure_logging(level='INFO')

treedb.set_root('../glottolog')

treedb.set_engine('treedb.sqlite3')

treedb.load()

treedb.check()

update = (sa.update(treedb.raw.Value)
          .where(sa.select(1)
                 .where(treedb.raw.Value.option_id == treedb.raw.Option.id)
                 .where(treedb.raw.Option.section == 'classification')
                 .where(treedb.raw.Option.option.in_(['sub', 'family']))
                 .exists())
          .where(treedb.raw.Value.value.like('%  %'))
          .values(value=sa.func.replace(treedb.raw.Value.value, '  ', ' ')))

print(update)

with treedb.connect() as conn:
    result = conn.execute(update)
    conn.commit()

print('', f'{result.rowcount:d} lines replaced', sep='\n')  # 42, 0

#print('', f"{treedb.write_files(source='raw')} files written", sep='\n')  # 42, 0
