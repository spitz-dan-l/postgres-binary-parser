from postgres_binary_parser.schema import *
from functools import singledispatch

@singledispatch
def col_sql_spec(col):
    raise NotImplementedError()

@col_sql_spec.register(cat)
def _(self):
    sql = '{} TEXT'.format(pandas_name_to_psql(self.name))
    return sql

@col_sql_spec.register(id_)
def _(self):
    result = '{} BIGINT'.format(pandas_name_to_psql(self.name))
    return result

@col_sql_spec.register(dt)
def _(self):
    sql = '{} TIMESTAMP'.format(pandas_name_to_psql(self.name))
    return sql
    
@col_sql_spec.register(delta)
def _(self):
    return '{} BIGINT'.format(pandas_name_to_psql(self.name))

@col_sql_spec.register(num)
def _(self):
    if self.options.get('int', False):
        return '{} BIGINT'.format(pandas_name_to_psql(self.name))
    else:
        return '{} DOUBLE PRECISION'.format(pandas_name_to_psql(self.name))

@col_sql_spec.register(bool_)
def _(self):
    return '{} BOOLEAN'.format(pandas_name_to_psql(self.name))

def psql_name_to_pandas(col_name):
    return col_name.replace('__', '.')

def pandas_name_to_psql(col_name):
    return col_name.replace('.', '__')

def psql_rename_df(df):
    df.columns = [pandas_name_to_psql(col) for col in df.columns]
    return df

def pandas_rename_df(df):
    df.columns = [psql_name_to_pandas(col) for col in df.columns]
    return df

def psql_rename_schema(schema):
    psql_schema = schema.rename(schema, pandas_name_to_psql(schema.name),
        rename_cols={col: pandas_name_to_psql(col) for col in schema.col_names()})
    return psql_schema

class SchemaSqlGenerator:
    def __init__(self, schema):
        self.schema = schema
        self.psql_schema = psql_rename_schema(schema)

    def col_spec(self):
        col_specs = []
        for col in self.psql_schema.cols:
            col_specs.append(col_sql_spec(col))
        return ', '.join(col_specs)

    def col_names(self):
        return ', '.join(self.psql_schema.col_names())

    def table_name(self):
        sql = ''
        db_schema = self.psql_schema.options.get('db_schema', None)
        if db_schema is not None:
            sql += db_schema + '.'
        sql += self.psql_schema.name
        return sql

    def create_table(self):
        temp_sql = 'TEMPORARY ' if self.psql_schema.options.get('temporary') else ''

        sql = 'CREATE {}TABLE IF NOT EXISTS {} ({})'.format(temp_sql, self.table_name(), self.col_spec())
        return sql

    def select_all(self):
        return 'SELECT {} FROM {}'.format(self.col_names(), self.table_name())

    def copy_from(self):
        return "COPY {} ({}) FROM STDIN WITH BINARY".format(self.table_name(), self.col_names())

    def copy_to(self):
        return "COPY ({}) TO STDOUT WITH BINARY".format(self.select_all())
try:
    from sqlalchemy import sql
except ImportError:
    sql = None

if sql is not None:
    sa_type_2_col_type = {
        sql.sqltypes.Integer: num,
        sql.sqltypes.String: cat,
        sql.sqltypes.Date: dt,
        sql.sqltypes.DateTime: dt,
        sql.sqltypes.Interval: delta,
        sql.sqltypes.Numeric: num,
        sql.sqltypes.Boolean: bool_
    }

    def sqlalchemy_table_as_schema(table):
        schema_cols = []
        for sa_col in table.c:
            for sa_type, col_type in sa_type_2_col_type.items():
                if isinstance(sa_col.type, sa_type):
                    if isinstance(sa_col.type, sql.sqltypes.Integer) and (sa_col.primary_key or sa_col.foreign_keys):
                        schema_cols.append(id_(sa_col.name))
                    else:
                        schema_cols.append(col_type(sa_col.name))
                    break
        options = {}
        if table.schema is not None:
            options['db_schema'] = table.schema
        s = Schema(table.name, schema_cols, options=options)
        return s