from .dataframe import DataFrame
from pyarrow.dataset import dataset
import pyarrow.parquet as parq
from pyarrow import LocalFileSystem

class DataFrameReader(object):
    
    def __init__(self, sparkSession):
        self._format = "parquet"
        self.sparkSession = sparkSession

    def format(self, format):
        self._format = format
        return self

    def schema(self, schema):
        self._schema = schema
        return self

    def options(self, **options):
        self._options = options
        return self

    def _df(self, path=None):
        return DataFrame(self.sparkSession, dataset(path, format=self._format))

    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        path : str or list, optional
            optional string or a list of string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source. Default to 'parquet'.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Examples
        --------
        >>> df = spark.read.format("parquet").load('python/test_support/sql/parquet_partitioned',
        ...     opt1=True, opt2=1, opt3='str')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]

        >>> df = spark.read.format('json').load(['python/test_support/sql/people.json',
        ...     'python/test_support/sql/people1.json'])
        >>> df.dtypes
        [('age', 'bigint'), ('aka', 'string'), ('name', 'string')]
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if isinstance(path, str):
            return self._df(path)
        elif path is not None:
            if type(path) != list:
                path = [path]
            # TODO: wildcard to multiple paths
            return self._df(path)
        else:
            return self._df()
