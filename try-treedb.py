#!/usr/bin/env python3

"""Usage examples."""

import pprint

import sqlalchemy as sa
import treedb

treedb.configure(log_sql=False)

pprint.pprint(dict(treedb.iterlanguoids(limit=1)))

engine = treedb.load()

treedb.check()  # run sanity checks

treedb.print_rows(sa.select(treedb.Languoid).order_by('id').limit(5))

query = treedb.get_example_query()  # big example query containing 'everything'

qf = treedb.pd_read_sql(query, index_col='id')
if qf is not None:
    qf.info(memory_usage='deep')

lf = treedb.pd_read_languoids()
if lf is not None:
    lf.info(memory_usage='deep')

#treedb.write_csv()
#treedb.write_languoids()

#treedb.write_files()

import treedb.raw
pprint.pprint(dict([next(treedb.raw.fetch_records())]))

#treedb.raw.write_files()

#treedb.files.roundtrip()
