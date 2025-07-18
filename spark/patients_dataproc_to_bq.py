import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, trim

# SETUP

parser = argparse.ArgumentParser()
parser.add_argument('--bq_transfer_bucket', required=True)
parser.add_argument('--csv', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

bq_transfer_bucket = args.bq_transfer_bucket
patients_file = args.csv
output = args.output


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName('patients_data_processing') \
    .getOrCreate()


patients_schema = StructType([
    StructField("Id", StringType()),
    StructField("BIRTHDATE", DateType()),
    StructField("DEATHDATE", DateType()),
    StructField("SSN", StringType()),
    StructField("DRIVERS", StringType()),
    StructField("PASSPORT", StringType()),
    StructField("PREFIX", StringType()),
    StructField("FIRST", StringType()),
    StructField("MIDDLE", StringType()),
    StructField("LAST", StringType()),
    StructField("SUFFIX", StringType()),
    StructField("MAIDEN", StringType()),
    StructField("MARITAL", StringType()),
    StructField("RACE", StringType()),
    StructField("ETHNICITY", StringType()),
    StructField("GENDER", StringType()),
    StructField("BIRTHPLACE", StringType()),
    StructField("ADDRESS", StringType()),
    StructField("CITY", StringType()),
    StructField("STATE", StringType()),
    StructField("COUNTY", StringType()),
    StructField("FIPS", StringType()),
    StructField("ZIP", IntegerType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("HEALTHCARE_EXPENSES", DoubleType()),
    StructField("HEALTHCARE_COVERAGE", DoubleType()),
    StructField("INCOME", DoubleType()),
])

logger.info(f"Reading {patients_file}...")
df = spark.read \
    .option("header", "true") \
    .option("dateFormat", "YYYY-MM-DD") \
    .schema(patients_schema) \
    .csv(patients_file)







# CLEANING

df = df.repartition(8)

pre_clean_record_count = df.count()
logger.info(f"Original record count: {pre_clean_record_count}")

string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
for column in string_columns:
    df = df.withColumn(
        column, 
        when((col(column) == "") | (col(column) == "null"), None).otherwise(trim(col(column)))
    )

# remove rows with NULL in a required field
logger.info("per the schema defined at https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary#patients\n" \
    "null values for required fields are not allowed")
required_fields = ["Id", "BIRTHDATE", "SSN", "FIRST", "LAST", "RACE", "ETHNICITY", 
                   "GENDER", "BIRTHPLACE", "ADDRESS", "CITY", "STATE", 
                   "HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE", "INCOME"]
for field in required_fields:
    df = df.filter(col(field).isNotNull())


null_records_removed = pre_clean_record_count - df.count()
logger.info(f"removed {null_records_removed} records with null values in required fields")
pre_clean_record_count = df.count()


logger.info("per the schema defined at https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary#patients\n" \
    "Gender must be M or F, Marital must be M or S\n" \
    "Healthcare_Expenses, Healthcare_Coverage, and Income must be >= 0\n")
df = df.dropDuplicates(["Id"]) \
    .filter(col("GENDER").isin(["M", "F"])) \
    .filter(col("MARITAL").isNull() | col("MARITAL").isin(["M", "S", "D", "W"])) \
    .filter(col("HEALTHCARE_EXPENSES") >= 0) \
    .filter(col("HEALTHCARE_COVERAGE") >= 0) \
    .filter(col("INCOME") >= 0)
# TODO: Check for unrealistic birth dates

# DEBUG: Show records that do not conform to the specifications
# df.subtract(df_cleaned).show(n=100,truncate=False)
# df = df_cleaned

malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"removed {malformed_records_removed} records not conforming to the specifications")






# OUTPUT

# df.registerTempTable('patients_data')

logger.info(f"Saving cleaned data to {output}...")
df.write.format('bigquery') \
  .mode('overwrite') \
  .option('table', output) \
  .option('temporaryGcsBucket', bq_transfer_bucket) \
  .save()

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")

# TODO: save the summary report to a file - include malformed_records_removed, null_records_removed current row count: df.count()

spark.stop()