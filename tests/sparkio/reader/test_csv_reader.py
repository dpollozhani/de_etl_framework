import pytest
from pyspark.sql import SparkSession
from src.sparkio.reader.csv_reader import CSVReader


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture(scope="module")
def test_csv_path():
    # Create a sample CSV file for testing
    test_csv_path = "test_data.csv"
    with open(test_csv_path, "w") as test_file:
        test_file.write("id,name,age\n1,John,25\n2,Jane,30\n3,Bob,22\n")
    yield test_csv_path
    # Delete the tests CSV file after testing
    import os
    if os.path.exists(test_csv_path):
        os.remove(test_csv_path)


@pytest.fixture(scope="module")
def test_csv_path_no_header():
    # Create a sample CSV file for testing
    test_csv_path = "test_data_no_header.csv"
    with open(test_csv_path, "w") as test_file:
        test_file.write("1,John,25\n2,Jane,30\n3,Bob,22\n")
    yield test_csv_path
    # Delete the tests CSV file after testing
    import os
    if os.path.exists(test_csv_path):
        os.remove(test_csv_path)


def test_read_csv_with_header(spark_session, test_csv_path):
    csv_reader = CSVReader(path=test_csv_path, sep=",", header=True)
    result_df = csv_reader.read()
    expected_columns = ["id", "name", "age"]
    assert result_df.columns == expected_columns
    assert result_df.count() == 3  # Three rows in the tests CSV file


def test_read_csv_without_header(spark_session, test_csv_path_no_header):
    csv_reader = CSVReader(path=test_csv_path_no_header, sep=",", header=False)
    result_df = csv_reader.read()
    expected_columns = ["_c0", "_c1", "_c2"]  # Default column names without header
    assert result_df.columns == expected_columns
    assert result_df.count() == 3


def test_read_csv_with_custom_schema(spark_session, test_csv_path_no_header):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    custom_schema = StructType([
        StructField("person_id", IntegerType(), True),
        StructField("person_name", StringType(), True),
        StructField("person_age", IntegerType(), True)
    ])
    csv_reader = CSVReader(path=test_csv_path_no_header, sep=",", schema=custom_schema, header=False)
    result_df = csv_reader.read()
    expected_columns = ["person_id", "person_name", "person_age"]
    assert result_df.columns == expected_columns
    assert result_df.count() == 3
