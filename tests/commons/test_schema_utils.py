import pytest
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, \
    TimestampType, StructType, StructField
from src.commons.schema_utils import get_column_type, get_schema_from_mapping_file


@pytest.fixture(scope="module")
def test_yaml_path():
    # Create a sample YAML mapping file for testing
    test_yaml_path = "test_schema_mapping.yaml"
    with open(test_yaml_path, "w") as test_file:
        test_file.write(
            "column1:\n  data_type: string\n  nullable: true\n\ncolumn2:\n  data_type: integer\n  nullable: false\n")
    yield test_yaml_path
    # Delete the tests YAML mapping file after testing
    import os
    if os.path.exists(test_yaml_path):
        os.remove(test_yaml_path)


def test_get_column_type():
    assert get_column_type("string") == StringType()
    assert get_column_type("int") == IntegerType()
    assert get_column_type("integer") == IntegerType()
    assert get_column_type("long") == LongType()
    assert get_column_type("double") == DoubleType()
    assert get_column_type("float") == FloatType()
    assert get_column_type("boolean") == BooleanType()
    assert get_column_type("date") == DateType()
    assert get_column_type("timestamp") == TimestampType()
    assert get_column_type("unknown") == StringType()  # Default to StringType for unknown types


def test_get_schema_from_mapping_file(test_yaml_path):
    schema = get_schema_from_mapping_file(test_yaml_path)
    assert isinstance(schema, StructType)
    assert len(schema) == 2
    assert schema[0].name == "column1"
    assert schema[0].dataType == StringType()
    assert schema[0].nullable == True
    assert schema[1].name == "column2"
    assert schema[1].dataType == IntegerType()
    assert schema[1].nullable == False

# Add more tests cases based on your specific scenarios
