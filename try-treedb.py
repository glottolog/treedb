#!/usr/bin/env python3
# try-treedb.py - usage examples

import treedb

from treedb import Languoid, select

treedb.configure()

print(next(treedb.iterlanguoids()))

engine = treedb.load()

treedb.print_rows(select([Languoid]).order_by('id').limit(5))

query = treedb.get_query()  # big example query containing 'everything'

df = treedb.pd_read_sql(query, index_col='id')
if df is not None:
    df.info()

# run sanity checks
treedb.check()

#treedb.write_csv()

#treedb.languoids.write_json_csv(engine)
#treedb.languoids.compare_with_files()

#treedb.files.roundtrip()

#import treedb.raw
#print(next(treeedb.raw.iterrecords()))
#treedb.raw.write_files()
