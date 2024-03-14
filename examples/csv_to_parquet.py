from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from src.commons.file_utils import read_yaml
from src.sparkio.reader.csv_reader import CSVReader

# Create a Spark session
from src.sparkio.writer.parquet_writer import ParquetWriter
from src.transform.dataframe_transformer import src_to_target_map

spark = SparkSession.builder.appName("Read CSV with auto schema").getOrCreate()

csv_file_path = r'data/LoanStats_securev1_2018Q4.csv'  # change the file path with 'data/LoanStats_sample.csv' for sample file testing
parquet_folder_path = r"data\out\LoanStats_parquet"

csv_reader = CSVReader(path=csv_file_path, sep=",", header=True, extra_options={"comment":'N'})
df = csv_reader.read()

target_schema_file = r"schema/target_schema.yaml"
target_schema_map = read_yaml(target_schema_file)

transform_df:DataFrame = src_to_target_map(df, target_schema_map)

transform_df.printSchema()
transform_df.show()

# write to parquet
parquet_writer = ParquetWriter(spark, df=transform_df, path=parquet_folder_path, mode="overwrite")
parquet_writer.write()
