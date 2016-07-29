import psycopg2
import pandas as pd
import numpy as np

from postgres_binary_parser.schema import Schema, num, cat, dt, delta, bool_, id_
from postgres_binary_parser.psql_binary_datastore import PsqlBinaryDataStore
from postgres_binary_parser.sql_generator import SchemaSqlGenerator

from random import choice
import io

test_schema = Schema('test', [
    num('a', int=True),
    cat('b'),
    num('c'),
    num('d'),
    num('e'),
    num('f'),
    dt('g'),
    delta('h'),
    bool_('i')
])

print('using test schema:')
print(test_schema)

test_df = pd.DataFrame(index=list(range(1000)))
test_df['a'] = 88
s = 'abcdefghijklmnopqrs'
test_df['b'] = [''.join(choice(s) for n in range(5)) for m in range(1000)]
test_df['c'] = 7
test_df.loc[np.unique(np.random.randint(0, 1000, size=100)), 'c'] = np.nan
test_df['d'] = np.random.randint(0, 10000000, size=1000)
test_df['e'] = np.random.randint(0, 10000000, size=1000)
test_df['f'] = np.random.randint(0, 10000000, size=1000)
test_df['g'] = pd.to_datetime('now')
test_df['h'] = pd.to_timedelta('15 days 5 hours 30 minutes')
test_df['i'] = True

conn = psycopg2.connect(database='dspitz', user='dspitz')
cur = conn.cursor()
cur.execute('drop table if exists test')

sql_gen = SchemaSqlGenerator(test_schema)

create_table_sql = sql_gen.create_table()
print(create_table_sql)
cur.execute(create_table_sql)

f = io.BytesIO()
bin_store = PsqlBinaryDataStore(test_schema, f)
bin_store.store(test_df)
f.seek(0)

copy_from_sql = sql_gen.copy_from()
print(copy_from_sql)
cur.copy_expert(copy_from_sql, f)
print('Successfully wrote binary data to Postgres from a DataFrame')

f = io.BytesIO()
copy_to_sql = sql_gen.copy_to()
print(copy_to_sql)
cur.copy_expert(copy_to_sql, f)
f.seek(0)

bin_store = PsqlBinaryDataStore(test_schema, f)
decoded_df = bin_store.load()
print('Successfully loaded binary from Postgres and parsed it into a DataFrame')

