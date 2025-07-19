import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, DecimalType
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
    .config("spark.sql.caseSensitive", "false") \
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
    StructField("HEALTHCARE_EXPENSES", DecimalType(38, 18)),
    StructField("HEALTHCARE_COVERAGE", DecimalType(38, 18)),
    StructField("INCOME", DecimalType(38, 2)),
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

# Apply filters
logger.info("per the schema defined at https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary#patients\n" \
    "Gender must be M or F, Marital must be M or S\n" \
    "Healthcare_Expenses, Healthcare_Coverage, and Income must be >= 0\n")

filters = {
    "duplicate_ids": lambda df: df.dropDuplicates(["Id"]),
    "valid_gender": lambda df: df.filter(col("GENDER").isin(["M", "F"])),
    "valid_marital": lambda df: df.filter(col("MARITAL").isNull() | col("MARITAL").isin(["M", "S", "D", "W"])),
    "healthcare_expenses_nonNeg": lambda df: df.filter(col("HEALTHCARE_EXPENSES") >= 0),
    "healthcare_coverage_nonNeg": lambda df: df.filter(col("HEALTHCARE_COVERAGE") >= 0),
    "income_nonNeg": lambda df: df.filter(col("INCOME") >= 0),
}

# 0=none, 1=counts, 2=counts+print_sample_of_removed
DEBUG_LEVEL_OF_FILTERING = 2
for filter_name, filter_func in filters.items():
    if DEBUG_LEVEL_OF_FILTERING == 0:
        df = filter_func(df)
    elif DEBUG_LEVEL_OF_FILTERING == 1:
        before_count = df.count()
        df = filter_func(df)
        logger.info(f"{filter_name}: removed {before_count - df.count()} records")
    elif DEBUG_LEVEL_OF_FILTERING == 2:
        before_count = df.count()
        df_cleaned = filter_func(df)
        cleaned_count = before_count - df_cleaned.count()
        logger.info(f"{filter_name}: removed {cleaned_count} records")
        if cleaned_count > 0:
            df.subtract(df_cleaned).show(5, truncate=True)
        df = df_cleaned

# TODO: Check for unrealistic birth dates

malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"Filtered a total of {malformed_records_removed} records not conforming to the specifications")

final_record_count = df.count()
logger.info(f"Final record count: {final_record_count}")



# OUTPUT

# `useAvroLogicalTypes` = true to handle Dates, Timestamps and DecimalType correctly in BigQuery
logger.info(f"Saving cleaned data to {output}...")
df.write.format('bigquery') \
  .mode('overwrite') \
  .option('table', output) \
  .option('temporaryGcsBucket', bq_transfer_bucket) \
  .option('useAvroLogicalTypes', 'true') \
  .save()

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")
logger.info(f"Data cleaning summary:")
logger.info(f"  - Original records: {malformed_records_removed + null_records_removed}")
logger.info(f"  - Null records removed: {null_records_removed}")
logger.info(f"  - Malformed records removed: {malformed_records_removed}")
logger.info(f"  - Final records: {final_record_count}")

# TODO: save the summary report to a file - include malformed_records_removed, null_records_removed current row count: df.count()

spark.stop()