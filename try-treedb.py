#!/usr/bin/env python3
# try-treedb.py - usage examples

import treedb

from treedb import Languoid, select

treedb.configure()

print(next(treedb.iterlanguoids()))

engine = treedb.load()

treedb.print_rows(select([Languoid]).order_by('id').limit(5))

tree = Languoid.tree(include_self=True, with_steps=True, with_terminal=True)
treedb.print_rows(tree.select().where(tree.c.child_id == 'book1242'))
treedb.print_rows(tree.select().where(tree.c.child_id == 'ramo1244'))

print(next(treedb.iterdescendants(parent_level='top', child_level='language')))

query = treedb.get_query()  # big example query containing 'everything'

df = treedb.pd_read_sql(query, index_col='id')
if df is not None:
    df.info()

# run sanity checks
treedb.check()

#treedb.write_csv()

#treedb.export()
#treedb.dump_sql()

#treedb.files.roundtrip()

#from treedb import raw

#print(next(raw.iterrecords()))
#print(next(treedb.iterlanguoids(engine)))
#raw.print_stats()
#raw.write_raw_csv()

#treedb.languoids.compare_with_files()
#treedb.languoids.write_json_csv(engine)

#raw.drop_duplicate_sources()
#raw.drop_duplicated_triggers()
#raw.drop_duplicated_crefs()
#raw.write_files()

#import treedb.raw.fixes
#treedb.raw.fixes.update_countries()
#treedb.raw.fixes.update_wikidata_links()
#treedb.raw.write_files()
