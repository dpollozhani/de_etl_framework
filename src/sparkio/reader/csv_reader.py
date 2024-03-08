from pyspark.sql import SparkSession

from src.commons.schema_utils import get_schema_from_mapping_file


class CSVReader:
    def __init__(self, path, sep=",", header=True, schema_file=None, schema=None, extra_options=None):
        """
        Initialize the CSVReader.

        Parameters:
        - path (str): Path to the CSV file.
        - sep (str, optional): Delimiter for the CSV file. Default is ",".
        - header (bool, optional): Whether the CSV file has a header row. Default is True.
        - schema_file (str, optional): Path to the schema file.
        - schema (pyspark.sql.types.StructType, optional): User-defined schema for the DataFrame.
        - extra_options (dict, optional): Additional options to pass to the Spark DataFrameReader.

        Example:
        ```
        csv_reader = CSVReader(path='data.csv', sep=',', header=True, schema_file=None, schema=None, extra_options={})
        ```
        """
        self.path = path
        self.sep = sep
        self.header = header
        self.schema_file = schema_file
        self.schema = schema
        self.extra_options = extra_options
        self.spark = SparkSession.builder.getOrCreate()

    def read(self):
        """
        Read data from the CSV file and return a DataFrame.

        Returns:
        - pyspark.sql.DataFrame: DataFrame containing the data from the CSV file.
        """
        reader = self.spark.read.format("csv").option("sep", self.sep)
        if self.schema:
            reader = reader.schema(self.schema)
        elif self.schema_file:
            schema = get_schema_from_mapping_file(self.schema_file)
            reader = reader.schema(schema)
        if self.header:
            reader = reader.option("header", self.header)
        if self.extra_options:
            reader = reader.options(**self.extra_options)
        return reader.load(self.path)
