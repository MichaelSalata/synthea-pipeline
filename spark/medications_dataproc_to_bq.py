import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, when, trim

# SETUP

parser = argparse.ArgumentParser()
parser.add_argument('--bq_transfer_bucket', required=True)
parser.add_argument('--csv', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

bq_transfer_bucket = args.bq_transfer_bucket
medications_file = args.csv
output = args.output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName('medications_data_processing') \
    .getOrCreate()

# Define schema based on the data dictionary
medications_schema = StructType([
    StructField("START", TimestampType()),
    StructField("STOP", TimestampType()),
    StructField("PATIENT", StringType()),
    StructField("PAYER", StringType()),
    StructField("ENCOUNTER", StringType()),
    StructField("CODE", StringType()),
    StructField("DESCRIPTION", StringType()),
    StructField("BASE_COST", DoubleType()),
    StructField("PAYER_COVERAGE", DoubleType()),
    StructField("DISPENSES", DoubleType()),
    StructField("TOTALCOST", DoubleType()),
    StructField("REASONCODE", StringType()),
    StructField("REASONDESCRIPTION", StringType()),
])

logger.info(f"Reading {medications_file}...")
df = spark.read \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
    .schema(medications_schema) \
    .csv(medications_file)


# CLEANING

df = df.repartition(8)

pre_clean_record_count = df.count()
logger.info(f"Original record count: {pre_clean_record_count}")

# Clean string columns - convert empty strings and "null" to None, and trim whitespace
string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
for column in string_columns:
    df = df.withColumn(
        column, 
        when((col(column) == "") | (col(column) == "null"), None).otherwise(trim(col(column)))
    )

# Remove rows with NULL in required fields
logger.info("Per the medications data dictionary, removing records with null values in required fields...")
required_fields = ["START", "PATIENT", "PAYER", "ENCOUNTER", "CODE", "DESCRIPTION", 
                  "BASE_COST", "PAYER_COVERAGE", "DISPENSES", "TOTALCOST"]

for field in required_fields:
    df = df.filter(col(field).isNotNull())

null_records_removed = pre_clean_record_count - df.count()
logger.info(f"Removed {null_records_removed} records with null values in required fields")
pre_clean_record_count = df.count()


# Apply filters

filters = {
    "base_cost_nonNeg": lambda df: df.filter(col("BASE_COST") >= 0),
    "payer_coverage_nonNeg": lambda df: df.filter(col("PAYER_COVERAGE") >= 0),
    "dispenses_nonNeg": lambda df: df.filter(col("DISPENSES") >= 0),
    "totalcost_nonNeg": lambda df: df.filter(col("TOTALCOST") >= 0),
    "date_range_stop_after_start": lambda df: df.filter(col("STOP").isNull() | (col("STOP") >= col("START"))),
    "payer_coverage_<=_totalcost": lambda df: df.filter(col("PAYER_COVERAGE") <= col("TOTALCOST")),
    "dispenses_positive": lambda df: df.filter(col("DISPENSES") > 0),
    "totalcost_consistent_with_base_and_dispenses": lambda df: df.filter(
        (col("TOTALCOST") >= col("BASE_COST")) & 
        (col("TOTALCOST") <= col("BASE_COST") * col("DISPENSES") * 1.1)  # Allow 10% tolerance for rounding
    ),
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

malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"Filtered a total of {malformed_records_removed} records not conforming to the specifications")

final_record_count = df.count()
logger.info(f"Final record count: {final_record_count}")



# OUTPUT

logger.info(f"Saving cleaned data to {output}...")
df.write.format('bigquery') \
  .mode('overwrite') \
  .option('table', output) \
  .option('temporaryGcsBucket', bq_transfer_bucket) \
  .save()

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")
logger.info(f"Data cleaning summary:")
logger.info(f"  - Original records: {pre_clean_record_count + null_records_removed}")
logger.info(f"  - Null records removed: {null_records_removed}")
logger.info(f"  - Malformed records removed: {malformed_records_removed}")
logger.info(f"  - Final records: {final_record_count}")

spark.stop()
