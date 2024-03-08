from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, \
    TimestampType, StructType, StructField
from src.commons.file_utils import read_yaml

# Mapping of string representations to PySpark SQL types
data_type_mapping = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    # Add more data types as required
}


def get_column_type(dtype):
    """
        Get the corresponding PySpark data type for a given data type from the data_type_mapping.

        Parameters:
        - dtype (str): The data type.

        Returns:
        - DataType: The PySpark data type.

        Example:
        data_type = get_column_type("string")
    """
    return data_type_mapping.get(dtype, StringType())


def get_schema_from_mapping_file(file_path):
    """
        Generate a PySpark StructType schema based on the information provided in a YAML mapping file.

        Parameters:
        - file_path (str): The path to the YAML mapping file.

        Returns:
        - StructType: PySpark StructType schema.

        Example:
        schema = get_schema_from_mapping_file("schema_mapping.yaml")
    """
    field_map = read_yaml(file_path)
    schema = StructType(
        [StructField(name, data_type_mapping.get(details['data_type'], StringType()), details['nullable']) for
         name, details
         in field_map.items()])
    return schema
