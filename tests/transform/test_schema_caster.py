import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.transform.schema_caster import cast_column_types

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_cast_column_types(spark_session):
    data = [("John", "25"), ("Jane", "30")]
    schema = StructType([StructField("name", StringType(), True), StructField("age", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)

    column_map = {"name": {"data_type": "string"}, "age": {"data_type": "integer"}}
    result_df = cast_column_types(df, column_map)

    # Check the casted data types
    assert result_df.schema["name"].dataType == StringType()
    assert result_df.schema["age"].dataType == IntegerType()

    # Check the casted values
    assert result_df.select("name").collect()[0]["name"] == "John"
    assert result_df.select("age").collect()[0]["age"] == 25

# Add more tests cases based on your specific scenarios
