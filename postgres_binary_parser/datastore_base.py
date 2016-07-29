class DataStore:
    """Base class - defines the DataStore abstraction.
    A DataStore is an adapter between a pandas DataFrame and a storage medium.
    Examples of storage media: HDF5 file format, SQL databases backed by SqlAlchemy,
     PostgreSQL databases."""
    def __init__(self, schema):
        self.schema = schema

    def storage_target(self):
        raise NotImplementedError()

    def load(self):
        result = self._load()
        self.schema.conform_df(result)
        return result

    def store(self, df):
        df = df.copy()
        self.schema.conform_df(df, storage_target=self.storage_target())
        self._store(df)
        del df #delete our copy

    def append(self, df):
        df = df.copy()
        self.schema.conform_df(df, storage_target=self.storage_target())
        self._append(df)
        del df #delete our copy

    def query(self, *args, **kwds):
        result = self._query(*args, **kwds)
        try:
            query_output_schema = self._query_output_schema(*args, **kwds)
        except NotImplementedError:
            pass
        else:
            query_output_schema.conform_df(result)

        return result

    def delete(self):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def _load(self):
        raise NotImplementedError()

    def _load_dask(self):
        raise NotImplementedError()

    def _store(self, df):
        raise NotImplementedError()

    def _append(self, df):
        raise NotImplementedError()

    def _query(self, *args, **kwds):
        raise NotImplementedError()

    def _query_output_schema(self, *args, **kwds):
        raise NotImplementedError()
