from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, date_format
from pyspark.sql.types import DateType
import datetime

from src.transform.schema_caster import cast_column_types

DEFAULT_DATE_FORMATS = ["%Y-%m-%d", "%b-%y", "%d-%b", "%m/%d/%Y","%d-%b-%y"]


def add_columns_with_defaults(df, column_map):
    """
        Add columns with default values to a DataFrame based on the provided column_map.

        Parameters:
        - df (DataFrame): The input DataFrame.
        - column_map (dict): A dictionary where keys are column names and values are dictionaries
                            with 'data_type' representing the desired data type and 'default'
                            representing the default value for the column.

        Returns:
        - DataFrame: The DataFrame with added columns and default values.

        Example:
        df = add_columns_with_defaults(df, {"col1": {"data_type": "string", "default": "default_value"}})
    """
    for col_name, col_details in column_map.items():
        df = df.withColumn(col_name,
                           lit(col_details.get("default", None)).cast(col_details["data_type"]).alias(col_name))
    return df


# Define a UDF to parse different date formats
def parse_date(date_str, expected_formats):
    if date_str is None:
        return None
    default_formats = expected_formats + [ d for d in DEFAULT_DATE_FORMATS if d not in expected_formats]
    for format_str in default_formats:
        try:
            # Try parsing with the current format
            return datetime.datetime.strptime(date_str, format_str).date()
        except ValueError:
            pass  # Continue to the next format if parsing fails
    # If parsing fails for all formats, return null
    return None

def apply_date_format_transformation(df, target_columns):
    """
        Apply date format transformations to specified columns in a DataFrame.
        - Select all date (data_type as date ) columns which has transformation as date_format
        - Parse the date based on user provided formats if any then default formats
        - send the date


        Parameters:
        - df (DataFrame): The input DataFrame.
        - target_columns (dict): A dictionary where keys are column names and values are dictionaries
                                 specifying transformations. Columns with 'transformation' set to 'date_format'
                                 will be transformed using the specified 'date_format' pattern.

        Returns:
        - DataFrame: The DataFrame with applied date format transformations.

        Example:
        df = apply_date_format_transformation(df, {"column_name": {"data_type": "date", "transformation": "date_format", "source_date_formats": ["yyyy-MM-dd"]}})
    """
    for col_name, transformation_info in target_columns.items():
        if transformation_info["data_type"].lower() == "date" and transformation_info.get("transformation", "") == "date_format":
            expected_date_formats = transformation_info.get("source_date_formats", [])
            parse_date_udf = udf(lambda date_str: parse_date(date_str, expected_date_formats), DateType())
            df = df.withColumn(col_name, parse_date_udf(col(col_name)))
    return df


def compare_tgt_to_src_columns(src, tgt):
    """
       Compare source and target column lists and identify available and missing columns.

       Parameters:
       - src (list): List of column names in the source DataFrame.
       - tgt (list): List of column names in the target DataFrame.

       Returns:
       - tuple: A tuple containing lists of available and missing columns.

       Example:
       available_cols, missing_cols = compare_tgt_to_src_columns(["col1", "col2"], ["col2", "col3"])
       print(available_cols) # ['col2']
       print(missing_cols)  # ['col3']
   """
    print(src, tgt)
    available_columns = list(set(src) & set(tgt))
    missing_columns = set(tgt) - set(src)
    return available_columns, missing_columns


def src_to_target_map(df: DataFrame, target_map: dict):
    """
        Transform a source DataFrame to match a target schema map by selecting available columns,
        adding default values for missing columns, and applying date format transformations.

        Parameters:
        - df (DataFrame): The source DataFrame.
        - target_map (dict): A dictionary representing the target schema map.

        Returns:
        - DataFrame: The transformed DataFrame based on the target schema map.

        Example:
        transformed_df = src_to_target_map(df, {"col1": {"data_type": "string"}, "col2": {"data_type": "integer"}})
    """
    src_columns = df.columns
    tgt_columns = list(target_map.keys())
    available_columns, missing_columns = compare_tgt_to_src_columns(src_columns, tgt_columns)

    # select available columns from source dataframe
    new_df = df.select(available_columns)

    # add default value for missing columns
    missing_columns_map = {k: v for k, v in target_map.items() if k in missing_columns}
    new_df = add_columns_with_defaults(new_df, missing_columns_map)

    # apply date transformations
    new_df = apply_date_format_transformation(new_df, target_map)
    new_df = cast_column_types(new_df, target_map)
    return new_df

