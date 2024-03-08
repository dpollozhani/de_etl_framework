from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame


class ParquetWriter:
    def __init__(self, spark:SparkSession, df:DataFrame, path, mode="append", num_of_files=None, extra_options=None):
        """
        Initialize the ParquetWriter.

        Parameters:
        - spark (SparkSession): The Spark session to use for writing Parquet files.
        - df (DataFrame): The input DataFrame to be written to Parquet.
        - path (str): The path where the Parquet files will be saved.
        - mode (str, optional): The mode for saving data. Default is "append".
        - num_of_files (int, optional): The number of output files to be generated (coalesce). Default is None.
        - extra_options (dict, optional): Additional options to pass to the DataFrameWriter. Default is None.

        Example:
        ```
        writer = ParquetWriter(spark, df, "/path/to/parquet", mode="overwrite", num_of_files=1, extra_options={"compression": "gzip"})
        writer.write()
        ```
        """
        self.path = path
        self.df = df
        self.num_of_files = num_of_files
        self.extra_options = extra_options
        self.spark = spark
        self.mode = mode

    def write(self):
        """
        Write the DataFrame to Parquet files at the specified path.

        Returns:
        - None
        """
        if self.num_of_files:
            self.df = self.df.coalesce(self.num_of_files)
        writer = self.df.write.format("parquet")
        if self.extra_options:
            writer = writer.options(**self.extra_options)
        writer.save(self.path, mode=self.mode)
