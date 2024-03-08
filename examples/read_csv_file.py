from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Import CSVReader from the sparkio.reader module
from sparkio.reader.csv_reader import CSVReader

# Create a Spark session
spark = SparkSession.builder.appName("Read CSV with auto schema").getOrCreate()

# Example 1: Read CSV File with header
csv_file_path = r'data/emp.csv'
csv_reader = CSVReader(path=csv_file_path, header=True)
df = csv_reader.read()
df.show()

# Example 2: Read CSV File without header using schema mapping file
csv_file_path = r'data/emp_no_header.csv'
schema_file_path = r'schema/emp_schema.yaml'
csv_reader = CSVReader(path=csv_file_path, sep='|', schema_file=schema_file_path, header=False)
df = csv_reader.read()
df.show()

# Example 3: Read multiple CSV Files without header using schema object
# Define the schema using StructType and StructField
schema = StructType([StructField('employee_id', IntegerType(), True), StructField('employee_name', StringType(), True)])

csv_file_path = [r'data/emp_no_header.csv', r'data/emp_no_header2.csv']
csv_reader = CSVReader(path=csv_file_path, sep='|', schema=schema, header=False)
df = csv_reader.read()
df.show()

# Example 4: Read CSV Files with wild character expression
csv_file_path = r'data/emp_no_header*.csv'
csv_reader = CSVReader(path=csv_file_path, sep='|', schema=schema, header=False)
df = csv_reader.read()
df.show()
