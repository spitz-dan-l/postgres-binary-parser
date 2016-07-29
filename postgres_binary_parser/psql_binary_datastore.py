from postgres_binary_parser.datastore_base import DataStore
from postgres_binary_parser.schema import *

from functools import singledispatch

import pandas as pd
import numpy as np

from postgres_binary_parser import psql_binary

class PsqlBinaryDataStore(DataStore):
    def __init__(self, schema, file):
        super().__init__(schema)
        self.file = file

    def storage_target(self):
        return 'pandas'

    def _store(self, df):
        buf = psql_binary.write_binary_df(df, self.schema)
        self.file.write(buf)

    def _load(self):
        read_header(self.file)
        col_buffers = psql_binary.read_binary_df(self.file.getbuffer(), self.schema)
        df = binary_to_df(self.schema, col_buffers)
        return df

def read_header(f):
    head = b'PGCOPY\n\377\r\n\0'
    res = f.read(len(head))
    if res != head:
        raise ValueError('Invalid header at start of postgres binary file')
    f.read(8)

def binary_to_df(schema, col_buffers):
    df = pd.DataFrame()
    for i, col in enumerate(schema.cols):
        df[col.name] = binary_to_col(col, col_buffers[i, 0], col_buffers[i, 1])
    return df

@singledispatch
def binary_to_col(col, ser):
    raise NotImplementedError()

@binary_to_col.register(cat)
def _(col, b_arr, null_mask):
    u_arr = psql_binary.parse_cat_col(b_arr, null_mask.shape[0])
    u_ser = pd.Series.from_array(u_arr).astype('category')

    code_arr = np.full(null_mask.shape, -1, dtype='i')
    code_ser = pd.Series.from_array(code_arr)
    code_ser.loc[~null_mask] = u_ser.cat.codes

    ser = pd.Categorical.from_codes(code_ser, u_ser.cat.categories)
    return ser

@binary_to_col.register(id_)
def _(col, b_arr, null_mask):
    return binary_col_to_int(b_arr, null_mask)

@binary_to_col.register(num)
def _(col, b_arr, null_mask):
    if col.options.get('int', False):
        return binary_col_to_int(b_arr, null_mask)
    else:
        return binary_col_to_float(b_arr, null_mask)

def binary_col_to_int(b_arr, null_mask):
    arr = np.full(null_mask.shape, np.nan)
    ser = pd.Series.from_array(arr)
    ser.loc[~null_mask] = np.frombuffer(b_arr, dtype='>q')
    return ser

def binary_col_to_float(b_arr, null_mask):
    arr = np.full(null_mask.shape, np.nan)
    ser = pd.Series.from_array(arr)
    ser.loc[~null_mask] = np.frombuffer(b_arr, dtype='>d')
    return ser

POSTGRES_EPOCH_TIME = pd.to_datetime('2000/1/1').tz_localize('utc')
@binary_to_col.register(dt)
def _(col, b_arr, null_mask):
    arr = np.full(null_mask.shape, np.nan)
    ser = pd.Series.from_array(arr)
    ser.loc[~null_mask] = np.frombuffer(b_arr, dtype='>q')
    ser = pd.to_datetime(ser / 1000000 + POSTGRES_EPOCH_TIME.timestamp(), unit='s')
    return ser

@binary_to_col.register(delta)
def _(col, b_arr, null_mask):
    arr = np.full(null_mask.shape, np.nan)
    ser = pd.Series.from_array(arr)
    ser.loc[~null_mask] = np.frombuffer(b_arr, dtype='>q')
    ser = pd.to_timedelta(ser, unit='ns')
    return ser

@binary_to_col.register(bool_)
def _(col, b_arr, null_mask):
    arr = np.full(null_mask.shape, np.nan)
    ser = pd.Series.from_array(arr)
    ser.loc[~null_mask] = np.frombuffer(b_arr, dtype='uint8') != 0
    return ser