import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from src.transform.dataframe_transformer import add_columns_with_defaults, apply_date_format_transformation, \
    compare_tgt_to_src_columns, src_to_target_map


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


def test_create_dataframe(spark_session):
    df = spark_session.createDataFrame([
        (1, 2., 'string1'),
        (2, 3., 'string2'),
        (3, 4., 'string3')
    ], schema='a long, b double, c string')
    df.show()


def test_add_columns_with_defaults(spark_session):
    data = [("John", 25), ("Jane", 30)]
    schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
    df = spark_session.createDataFrame(data, "name string, age int")

    column_map = {"new_col": {"data_type": "string", "default": "default_value"}}
    result_df = add_columns_with_defaults(df, column_map)

    assert "new_col" in result_df.columns
    assert result_df.schema["new_col"].dataType == StringType()
    assert result_df.select("new_col").distinct().collect()[0]["new_col"] == "default_value"


def test_apply_date_format_transformation(spark_session):
    data = [("2022-01-01",)]
    schema = StructType([StructField("date_column", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)

    target_columns = {
        "date_column": {"data_type": "date", "transformation": "date_format", "source_date_formats": ["%Y-%m-%d"]}}
    result_df = apply_date_format_transformation(df, target_columns)

    assert result_df.select("date_column").distinct().collect()[0]["date_column"].strftime("%Y/%m/%d") == "2022/01/01"


def test_apply_date_format_transformation_column_with_multiple_formats(spark_session):
    data = [("2022-01-01",), ("Mar-12",), ("12-APR",)]
    schema = StructType([StructField("date_column", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)

    target_columns = {
        "date_column": {"data_type": "date", "transformation": "date_format", "source_date_formats": ["%Y-%m-%d"]}}
    result_df = apply_date_format_transformation(df, target_columns)
    expected_dates = {'2022/01/01', '2012/03/01', '1900/04/12'}
    actual_dates = {r["date_column"].strftime("%Y/%m/%d") for r in result_df.select("date_column").distinct().collect()}
    print(actual_dates)
    assert expected_dates == actual_dates


def test_compare_tgt_to_src_columns():
    src_columns = ["col1", "col2"]
    tgt_columns = ["col2", "col3"]

    available_cols, missing_cols = compare_tgt_to_src_columns(src_columns, tgt_columns)

    assert available_cols == ["col2"]
    assert missing_cols == {"col3"}


def test_src_to_target_map(spark_session):
    data = [("John", 25)]
    schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
    df = spark_session.createDataFrame(data, schema)

    target_map = {"name": {"data_type": "string"}, "age": {"data_type": "integer"},
                  "new_col": {"data_type": "string", "default": "default_value"}}
    transformed_df = src_to_target_map(df, target_map)

    assert set(transformed_df.columns) == {"name", "age", "new_col"}
    assert transformed_df.schema["new_col"].dataType == StringType()
    assert transformed_df.select("new_col").distinct().collect()[0]["new_col"] == "default_value"

# Add more tests cases based on your specific scenarios
