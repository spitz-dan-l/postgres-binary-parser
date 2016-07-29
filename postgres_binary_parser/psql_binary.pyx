from postgres_binary_parser.schema import cat, id_, num, dt, delta, bool_

from libc.string cimport memcpy
from cpython cimport array
import array

import numpy as np
cimport numpy as np
import pandas as pd
from libc.stdlib cimport malloc, free

cdef inline void read_reversed2(char *target, char *src, int *pos):
    cdef int pos_val = pos[0]
    target[0] = src[1+pos_val]
    target[1] = src[pos_val]
    pos[0] = pos_val+2
        
cdef inline void read_reversed4(char *target, char *src, int *pos):
    cdef int pos_val = pos[0]
    target[0] = src[3+pos_val]
    target[1] = src[2+pos_val]
    target[2] = src[1+pos_val]
    target[3] = src[pos_val]
    pos[0] = pos_val+4

def read_binary_df(char[:] f, object schema):
    head = b'PGCOPY\n\377\r\n\0'
    cdef int pos = len(head) + 8
    
    cdef int i
    
    cdef np.ndarray[dtype=object, ndim=2] col_buffers
    cdef np.ndarray[dtype=int, ndim=1] cat_cols
    cdef int num_cols = len(schema.cols)
    
    col_buffers = np.empty((num_cols, 2), dtype=np.object_)
    cat_cols = np.zeros(num_cols, dtype='i')
    for i in range(num_cols):
        col_buffers[i, 0] = array.array('b') #this is the raw bytes buffer
        col_buffers[i, 1] = array.array('b') #this is the null indicator
        
        cat_cols[i] = isinstance(schema.cols[i], cat)
    
    cdef short column_count
    cdef int field_size
    
    cdef array.array col_buf, null_buf
    cdef char is_field_null
    cdef int is_field_cat
    
    read_reversed2(<char *> &column_count, &f[0], &pos)
    
    while column_count != -1:
        for i in range(column_count):    
            read_reversed4(<char *> &field_size, &f[0], &pos)
            
            null_buf = <array.array>col_buffers[i, 1]
            col_buf = <array.array>col_buffers[i, 0]
            
            if field_size == -1: #it's null
                is_field_null = 1
            else:
                is_field_null = 0
                if cat_cols[i]:
                    array.extend_buffer(col_buf, <char *>&field_size, 4)
                array.extend_buffer(col_buf, &f[pos], field_size); pos += field_size
                
            array.extend_buffer(null_buf, &is_field_null, 1)
        
        read_reversed2(<char *> &column_count, &f[0], &pos)   
    
    for i in range(num_cols):
        col_buffers[i, 0] = np.frombuffer(col_buffers[i, 0], dtype='c') #this is the raw bytes buffer
        col_buffers[i, 1] = np.frombuffer(col_buffers[i, 1], dtype='bool') #this is the null indicator
    
    return col_buffers

def parse_cat_col(np.ndarray[dtype=char, ndim=1] values, int arr_size):
    cdef int field_size, pos = 0, i = 0
    cdef np.ndarray[dtype=object, ndim=1] fields
    cdef char *field
    
    fields = np.empty(arr_size, dtype=np.object_)
    
    while pos < len(values):
        memcpy(<char *>&field_size, &values[pos], 4); pos += 4
        field = &values[pos]
        fields[i] = <str>field[:field_size].decode('utf-8')
        pos += field_size
        i += 1
    fields.resize((i,))
    return fields


cdef int EIGHT = 8 
cdef int FOUR = 4
cdef int ONE = 1
cdef int ZERO = 0

cdef short NEGONE_short = -1
cdef int NEGONE_int = -1

cdef inline void write_reversed1(array.array target, char *src):
    array.extend_buffer(target, src, 1)

cdef inline void write_reversed2(array.array target, char *src):
    cdef char[2] tmp_buf
    tmp_buf[0] = src[1]
    tmp_buf[1] = src[0]

    array.extend_buffer(target, tmp_buf, 2)

cdef inline void write_reversed4(array.array target, char *src):
    cdef char[4] tmp_buf
    tmp_buf[0] = src[3]
    tmp_buf[1] = src[2]
    tmp_buf[2] = src[1]
    tmp_buf[3] = src[0]

    array.extend_buffer(target, tmp_buf, 4)

cdef inline void write_reversed8(array.array target, char *src):
    cdef char[8] tmp_buf
    tmp_buf[0] = src[7]
    tmp_buf[1] = src[6]
    tmp_buf[2] = src[5]
    tmp_buf[3] = src[4]
    tmp_buf[4] = src[3]
    tmp_buf[5] = src[2]
    tmp_buf[6] = src[1]
    tmp_buf[7] = src[0]
    
    array.extend_buffer(target, tmp_buf, 8)

cdef inline void write_null_field(array.array target):
    write_reversed4(<array.array>target, <char *>&NEGONE_int)
    
cdef inline void write_cat_field(array.array target, char *s, int field_len):
    write_reversed4(<array.array>target, <char *>&field_len)
    array.extend_buffer(target, s, field_len)

cdef inline void write_int_field(array.array target, long long field):
    write_reversed4(<array.array>target, <char *>&EIGHT)
    write_reversed8(<array.array>target, <char *>&field)

cdef inline void write_float_field(array.array target, double field):
    write_reversed4(<array.array>target, <char *>&EIGHT)
    write_reversed8(<array.array>target, <char *>&field)

cdef inline void write_bool_field(array.array target, np.uint8_t field):
    write_reversed4(<array.array>target, <char *>&ONE)
    write_reversed1(<array.array>target, <char *>&field)

cdef inline void write_row_header(array.array buf, short num_cols):
    write_reversed2(<array.array>buf, <char *>&num_cols)

cdef inline void write_file_header(array.array buf):
    cdef header_bytes = b'PGCOPY\n\377\r\n\0'
    cdef char *header = header_bytes
    array.extend_buffer(buf, header, len(header_bytes))
    write_reversed4(<array.array>buf, <char *>&ZERO)
    write_reversed4(<array.array>buf, <char *>&ZERO)

cdef enum ColType:
    CAT_TYPE = 0
    INT_TYPE = 1
    FLOAT_TYPE = 2
    DT_TYPE = 3
    DELTA_TYPE = 4
    BOOL_TYPE = 5

cdef inline ColType get_col_type(object col):
    if isinstance(col, cat):
        return ColType.CAT_TYPE
    elif isinstance(col, id_):
        return ColType.INT_TYPE
    elif isinstance(col, num):
        if col.options.get('int', False):
            return ColType.INT_TYPE
        else:
            return ColType.FLOAT_TYPE
    elif isinstance(col, dt):
        return ColType.DT_TYPE
    elif isinstance(col, delta):
        return ColType.DELTA_TYPE
    elif isinstance(col, bool_):
        return ColType.BOOL_TYPE

cdef class ColData:
    cdef ColType col_type
    cdef np.ndarray null_indicator
    cdef np.ndarray values

cdef class CatData(ColData):
    cdef np.ndarray length

cdef object POSTGRES_EPOCH_TIME = pd.to_datetime('2000/1/1').tz_localize('utc')

cdef inline ColData preprocess_column(object column, ColType col_type):
    cdef ColData col_data
    cdef CatData cat_data

    if col_type == ColType.CAT_TYPE:
        cat_data = CatData()
        cat_data.null_indicator = <np.ndarray> (column.cat.codes == -1).astype('uint8').values
        enc_ser = column[~cat_data.null_indicator].str.encode('utf-8')
        len_ser = enc_ser.str.len()
        cat_data.values = <np.ndarray> enc_ser.values
        cat_data.length = <np.ndarray> len_ser.values
        col_data = cat_data
    elif col_type == ColType.INT_TYPE:
        col_data = ColData()
        col_data.null_indicator = <np.ndarray> column.isnull().astype('uint8').values
        col_data.values = <np.ndarray> column.dropna().astype('int64').values
    elif col_type == ColType.FLOAT_TYPE:
        col_data = ColData()
        col_data.null_indicator = <np.ndarray> column.isnull().astype('uint8').values
        col_data.values = <np.ndarray> column.dropna().astype('float64').values
    elif col_type == ColType.DT_TYPE:
        col_data = ColData()
        col_data.null_indicator = <np.ndarray> column.isnull().astype('uint8').values
        nonnull = column.dropna()
        col_data.values = <np.ndarray> ((nonnull.dt.tz_localize('utc') - POSTGRES_EPOCH_TIME).dt.total_seconds() * 1000000).astype('int64').values
    elif col_type == ColType.DELTA_TYPE:
        col_data = ColData()
        col_data.null_indicator = <np.ndarray> column.isnull().astype('uint8').values
        col_data.values = <np.ndarray> (column.dropna().dt.total_seconds() * 1000000000).astype('int64').values
    elif col_type == ColType.BOOL_TYPE:
        col_data = ColData()
        col_data.null_indicator = <np.ndarray> column.isnull().astype('uint8').values
        col_data.values = <np.ndarray> column.dropna().astype('uint8').values
    col_data.col_type = col_type
    return col_data

cdef inline np.ndarray[dtype=object] df_to_col_datas(object df, object schema):
    cdef np.ndarray[dtype=object] col_datas
    cdef int i, num_cols = len(schema.cols), num_rows = len(df)
    cdef ColType col_type

    col_datas = np.empty(num_cols, dtype=np.object_)

    for i in range(num_cols):
        col_type = get_col_type(schema.cols[i])
        col_datas[i] = preprocess_column(df[df.columns[i]], col_type)
        
    return col_datas

cdef inline int process_field(array.array buf, ColData col_data, Py_ssize_t *value_ptr, Py_ssize_t *null_ind_ptr) except -1:
    cdef np.ndarray[dtype=np.int64_t] int_values
    cdef np.ndarray[dtype=np.float64_t] float_values
    cdef np.ndarray[dtype=np.uint8_t] bool_values
    
    cdef np.ndarray[dtype=np.uint8_t] null_indicator
    
    cdef np.ndarray[dtype=object] cat_values

    cdef np.ndarray[dtype=np.int64_t] cat_lens
    cdef CatData cat_data

    cdef ColType col_type = col_data.col_type

    null_indicator = <np.ndarray[dtype=np.uint8_t]>col_data.null_indicator
    if null_indicator[null_ind_ptr[0]] == 1:
        write_null_field(buf)
        null_ind_ptr[0] += 1
        return 0

    if col_type == ColType.INT_TYPE or col_type == ColType.DT_TYPE or col_type == ColType.DELTA_TYPE:
        int_values = <np.ndarray[dtype=np.int64_t]> col_data.values
        write_int_field(<array.array>buf, int_values[value_ptr[0]])
    elif col_type == ColType.FLOAT_TYPE:
        float_values = <np.ndarray[dtype=np.float64_t]> col_data.values
        write_float_field(<array.array>buf, float_values[value_ptr[0]])
    elif col_type == ColType.CAT_TYPE:
        cat_data = <CatData>col_data
        cat_values = <np.ndarray[dtype=object]>cat_data.values
        cat_lens = <np.ndarray[dtype=np.int64_t]>cat_data.length
        write_cat_field(<array.array>buf, <char *>cat_values[value_ptr[0]], cat_lens[value_ptr[0]])
    elif col_type == ColType.BOOL_TYPE:
        bool_values = <np.ndarray[dtype=np.uint8_t]> col_data.values
        write_bool_field(<array.array>buf, bool_values[value_ptr[0]] != 0)
    value_ptr[0] += 1
    null_ind_ptr[0] += 1

    return 0

cdef inline columns_to_buffer(array.array buf, np.ndarray[dtype=object] col_datas, int num_rows):
    cdef short num_cols = col_datas.shape[0]
    cdef Py_ssize_t *value_ptrs = <Py_ssize_t *>malloc(sizeof(Py_ssize_t) * num_cols)
    cdef Py_ssize_t *null_ind_ptrs = <Py_ssize_t *>malloc(sizeof(Py_ssize_t) * num_cols)

    cdef int i

    cdef Py_ssize_t j

    for i in range(num_cols):
        value_ptrs[i] = 0
        null_ind_ptrs[i] = 0

    for j in range(num_rows):
        write_row_header(<array.array>buf, num_cols)
        for i in range(num_cols):
            process_field(<array.array>buf, <ColData>col_datas[i], &value_ptrs[i], &null_ind_ptrs[i])

    free(value_ptrs)
    free(null_ind_ptrs)

def write_binary_df(object df, object schema):
    cdef array.array buf = array.array('b')
    write_file_header(buf)

    cdef np.ndarray[dtype=object] col_datas = df_to_col_datas(df, schema)
    cdef int num_rows = len(df)

    columns_to_buffer(buf, col_datas, num_rows)
    write_row_header(<array.array>buf, NEGONE_short)
    
    return bytes(buf)



