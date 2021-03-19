# postgres_binary_parser
Cython implementation of a parser for PostgreSQL's COPY WITH BINARY format

Goes from binary to pandas DataFrame and back, faster than anything in pure python.

I have gotten sick of how unnecessarily slow data loading and storing is between Pandas and databases. By default all data is converted to python tuples containing python objects because that is the interchange format that dbapi2 uses. If you're just expecting to put the data directly into a dataframe, this is dumb and slow.

I wrote this module to leverage PostgreSQL's COPY ... WITH BINARY format. It uses Cython to quickly parse the binary format and deposit it directly into numpy arrays, and then directly into a pandas DataFrame. Since the bottlenecks are all written in Cython, it's fast. It is faster than pandas.read_csv, especially on string data. The payload size is also a lot smaller than a CSV file.

## Requirements
Python 3 only, boo boo.

Cython, numpy and pandas.

Since you are presumably using PostgreSQL, also psycopg2. Optional SqlAlchemy support.

## Installation

Clone this repo and `cd` inside. Then do

`pip install .`

(This compiles the Cython and makes `postgres_binary_parser` importable as a package.)

## Demo
For a demo, see `demo.py`. Shows that you can use this package to load and store:
- Numeric (BIGINT and DOUBLE PRECISION)
- Categorical/String (TEXT)
- Datetime (TIMESTAMP)
- Timedelta (BIGINT [nanosecond])
- Boolean (BOOLEAN)

Demo relies on `psycopg2`

The basic form of using this package is to construct a `PsqlBinaryDataStore` and call `.load()` or `.store()` on it.

## Other stuff
This package introduces a python-object representation for simple data schemas, where you can define datasets by the names of their columns and datatypes. It's supposed to be simple, readable and get out of the way, e.g.:

```
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
```

Included in the package is a function for converting a SqlAlchemy `Table` object to this `Schema` form. As such it is possible, though not super convenient, to use SqlAlchemy's ability to inspect a database and map it to Table objects to generate `Schema`s for tables you have never seen before, and load their contents with this package. 

## Alpha
This code is poorly organized/documented currently. It's lifted out of a larger project and cleaned up for this particular use case.
