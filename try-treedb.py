#!/usr/bin/env python3
# try-treedb.py - usage examples

import treedb

from treedb import Languoid, select

treedb.configure()

print(next(treedb.iterlanguoids()))

engine = treedb.load()

treedb.print_rows(select([Languoid]).order_by('id').limit(5))

print(next(treedb.iterdescendants(parent_level='top', child_level='language')))

query = treedb.get_query()  # big example query containing 'everything'

df = treedb.pd_read_sql(query, index_col='id')
if df is not None:
    df.info()

# run sanity checks
treedb.check()

#treedb.write_csv()

#treedb.dump_sql()

#treedb.export()

#treedb.languoids.write_json_csv(engine)
#treedb.languoids.compare_with_files()

#treedb.files.roundtrip()

#import treedb.raw

#print(next(treeedb.raw.iterrecords()))
#treedb.raw.print_stats()
#treedb.raw.write_raw_csv()

#treedb.raw.drop_duplicate_sources()
#treedb.raw.drop_duplicated_triggers()
#treedb.raw.drop_duplicated_crefs()
#treedb.raw.write_files()

#import treedb.raw.fixes

#treedb.raw.fixes.update_countries()
#treedb.raw.fixes.update_wikidata_links()
#treedb.raw.write_files()
