#!/usr/bin/env python3

"""Change endangeredlanguages.com links to HTTPS."""

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
                 .where(treedb.raw.Option.section == 'core')
                 .where(treedb.raw.Option.option == 'links')
                 .exists())
          .where(treedb.raw.Value.value.like('%http://endangeredlanguages.com/%'))
          .values(value=sa.func.replace(treedb.raw.Value.value,
                                        'http://', 'https://')))

print(update)

with treedb.connect() as conn:
    result = conn.execute(update)
    conn.commit()

print('', f'{result.rowcount:d} lines replaced', sep='\n')  # 3577, 0
#print('', f"{treedb.write_files(source='raw')} files written", sep='\n') # 3650, 0
