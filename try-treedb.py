# try-treedb.py - usage examples

import sqlalchemy as sa

import treedb
import treedb.backend as _backend
from treedb import engine, Languoid


print(next(treedb.iterlanguoids()))

treedb.load()

_backend.print_rows(sa.select([Languoid]).order_by(Languoid.id).limit(5))

tree = Languoid.tree(include_self=True, with_steps=True, with_terminal=True)
_backend.print_rows(tree.select().where(tree.c.child_id == 'book1242'))
_backend.print_rows(tree.select().where(tree.c.child_id == 'ramo1244'))

print(next(treedb.iterdescendants(parent_level='top', child_level='language')))

query = treedb.get_query()  # big example query containing 'everything'

try:
    import pandas as pd
except ImportError:
    pass
else:
    df = pd.read_sql_query(query, engine, index_col='id')
    df.info()

# run sanity checks
treedb.check()

#treedb.export_db()
#treedb.write_csv()
