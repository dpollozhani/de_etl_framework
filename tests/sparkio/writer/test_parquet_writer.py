import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.sparkio.writer.parquet_writer import ParquetWriter

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def create_sample_dataframe(spark_session):
    data = [("John", 25), ("Jane", 30)]
    schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
    return spark_session.createDataFrame(data, schema)

def test_parquet_writer_append(spark_session, tmp_path):
    df = create_sample_dataframe(spark_session)
    path = str(tmp_path / "test_parquet_writer")

    writer = ParquetWriter(spark_session, df, path, mode="append")
    writer.write()

    # Verify that the Parquet files were written
    loaded_df = spark_session.read.parquet(path)
    assert loaded_df.count() == 2

def test_parquet_writer_overwrite(spark_session, tmp_path):
    df = create_sample_dataframe(spark_session)
    path = str(tmp_path / "test_parquet_writer")

    # Create initial Parquet files
    initial_writer = ParquetWriter(spark_session, df, path, mode="overwrite")
    initial_writer.write()

    # Overwrite with new data
    new_data = [("Alice", 22), ("Bob", 35)]
    new_df = spark_session.createDataFrame(new_data, df.schema)

    overwrite_writer = ParquetWriter(spark_session, new_df, path, mode="overwrite")
    overwrite_writer.write()

    # Verify that the Parquet files were overwritten with new data
    loaded_df = spark_session.read.parquet(path)
    assert loaded_df.count() == 2
    assert loaded_df.select("name").collect()[0]["name"] == "Alice"

# Add more tests cases based on your specific scenarios
