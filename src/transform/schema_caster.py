from pyspark.sql.functions import col


def cast_column_types(df, column_map):
    """
        Cast the data types of specified columns in a DataFrame based on the provided column_map.

        Parameters:
        - df (DataFrame): The input DataFrame.
        - column_map (dict): A dictionary where keys are column names and values are dictionaries
                            with 'data_type' representing the desired data type.

        Returns:
        - DataFrame: The DataFrame with casted column types.

        Example:
        df = cast_column_types(df, {"col1": {"data_type": "string"}, "col2": {"data_type": "integer"}})
    """
    for col_name, column_details in column_map.items():
        df = df.withColumn(col_name, col(col_name).cast(column_details["data_type"]).alias(col_name))
    return df

