import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DecimalType
)
from pyspark.sql.functions import col, when, trim, regexp_replace

# SETUP

parser = argparse.ArgumentParser()
parser.add_argument('--bq_transfer_bucket', required=True)
parser.add_argument('--csv', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

bq_transfer_bucket = args.bq_transfer_bucket
organizations_file = args.csv
output = args.output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName('organizations_data_processing') \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()

# Define schema based on the data dictionary
organizations_schema = StructType([
    StructField("Id", StringType()),
    StructField("NAME", StringType()),
    StructField("ADDRESS", StringType()),
    StructField("CITY", StringType()),
    StructField("STATE", StringType()),
    StructField("ZIP", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("PHONE", StringType()),
    StructField("REVENUE", DecimalType(38, 18)),
    StructField("UTILIZATION", IntegerType()),
])

logger.info(f"Reading {organizations_file}...")
df = spark.read \
    .option("header", "true") \
    .schema(organizations_schema) \
    .csv(organizations_file)


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

# Clean phone numbers
df = df.withColumn(
    "PHONE",
    when(col("PHONE").isNotNull(), 
         regexp_replace(col("PHONE"), " Or .*", "")).otherwise(col("PHONE"))
)

# Remove rows with NULL in required fields
logger.info("Per the organizations data dictionary, removing records with null values in required fields...")
required_fields = ["Id", "NAME", "ADDRESS", "CITY", "REVENUE", "UTILIZATION"]

for field in required_fields:
    df = df.filter(col(field).isNotNull())

null_records_removed = pre_clean_record_count - df.count()
logger.info(f"Removed {null_records_removed} records with null values in required fields")
pre_clean_record_count = df.count()


# Apply filters
filters = {
    "Id_isUnique": lambda df: df.dropDuplicates(["Id"]),
    "revenue_nonNeg": lambda df: df.filter(col("REVENUE") >= 0),
    "utilization_nonNeg": lambda df: df.filter(col("UTILIZATION") >= 0),
    "lat_valid_range": lambda df: df.filter(col("LAT").isNull() | ((col("LAT") >= -90) & (col("LAT") <= 90))),
    "lon_valid_range": lambda df: df.filter(col("LON").isNull() | ((col("LON") >= -180) & (col("LON") <= 180))),
    "state_valid_format": lambda df: df.filter(col("STATE").isNull() | (col("STATE").rlike("^[A-Z]{2}$"))),
    "zip_valid_format": lambda df: df.filter(col("ZIP").isNull() | (col("ZIP").rlike("^[0-9]{5}([0-9]{4})?$"))),
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
        cleaned_count = df.count() - df_cleaned.count()
        logger.info(f"{filter_name}: removed {cleaned_count} records")
        if cleaned_count > 0:
            df.subtract(df_cleaned).show(5, truncate=True)
            df = df_cleaned

malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"Filtered a total of {malformed_records_removed} records not conforming to the specifications")

final_record_count = df.count()
logger.info(f"Final record count: {final_record_count}")


# OUTPUT

# `useAvroLogicalTypes` is set to true to handle Dates, Timestamps and DecimalType correctly in BigQuery
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
logger.info(f"  - Original records: {pre_clean_record_count + null_records_removed}")
logger.info(f"  - Null records removed: {null_records_removed}")
logger.info(f"  - Malformed records removed: {malformed_records_removed}")
logger.info(f"  - Final records: {final_record_count}")

spark.stop()
