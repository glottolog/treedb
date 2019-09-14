#!/usr/env/bin python
# try-treedb.py - usage examples

import logging

import treedb

from treedb import Languoid, select

logging.basicConfig(format='[%(levelname)s@%(name)s] %(message)s')
logging.captureWarnings(True)
logging.getLogger('treedb').setLevel(logging.INFO)

treedb.set_root('../glottolog')

print(next(treedb.iterlanguoids()))

#treedb.create_engine('treedb.sqlite3')

engine = treedb.load()

treedb.print_rows(select([Languoid]).order_by('id').limit(5))

tree = Languoid.tree(include_self=True, with_steps=True, with_terminal=True)
treedb.print_rows(tree.select().where(tree.c.child_id == 'book1242'))
treedb.print_rows(tree.select().where(tree.c.child_id == 'ramo1244'))

print(next(treedb.iterdescendants(parent_level='top', child_level='language')))

query = treedb.get_query()  # big example query containing 'everything'

try:
    import pandas as pd
except ImportError:
    pass
else:
    df = pd.read_sql_query(query, con=engine, index_col='id')
    df.info()

# run sanity checks
treedb.check()

#treedb.export()
#treedb.write_csv()

#treedb.files.roundtrip()

#from treedb import raw

#print(next(raw.iterrecords()))
#print(next(treedb.iterlanguoids(engine)))
#raw.print_stats()
#raw.checksum()
#raw.checksum(weak=True)
#raw.to_raw_csv()

#treedb.languoids.compare_with_raw()
#treedb.languoids.to_json_csv(engine)

#raw.drop_duplicate_sources()
#raw.drop_duplicated_triggers()
#raw.drop_duplicated_crefs()
#raw.to_files()

#import treedb.raw.fixes
#treedb.raw.fixes.update_countries()
#treedb.raw.to_files()
