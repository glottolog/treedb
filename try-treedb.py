#!/usr/env/bin python
# try-treedb.py - usage examples

import treedb

from treedb import Languoid, select

print(next(treedb.iterlanguoids()))

treedb.load()

treedb.print_rows(select([Languoid]).order_by(Languoid.id).limit(5))

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
    df = pd.read_sql_query(query, con=treedb.engine, index_col='id')
    df.info()

# run sanity checks
treedb.check()

#treedb.export()
#treedb.write_csv()

#treedb.files.roundtrip()

#from treedb import raw

#print(next(raw.iterrecords()))
#print(next(treedb.iterlanguoids(treedb.engine)))
#raw.print_stats()
#raw.to_raw_csv()

#treedb.languoids.compare_with_raw()
#treedb.languoids.to_json_csv(treedb.engine)

#raw.drop_duplicate_sources()
#raw.drop_duplicated_triggers()
#raw.drop_duplicated_crefs()
#raw.to_files()
