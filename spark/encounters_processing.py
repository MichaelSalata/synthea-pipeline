import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, when, trim, to_timestamp

# SETUP

parser = argparse.ArgumentParser()
parser.add_argument('--encounters_file', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

encounters_file = args.encounters_file
output = args.output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('encounters_data_processing') \
    .getOrCreate()

# Define schema based on the data dictionary
encounters_schema = StructType([
    StructField("Id", StringType()),
    StructField("START", TimestampType()),
    StructField("STOP", TimestampType()),
    StructField("PATIENT", StringType()),
    StructField("ORGANIZATION", StringType()),
    StructField("PROVIDER", StringType()),
    StructField("PAYER", StringType()),
    StructField("ENCOUNTERCLASS", StringType()),
    StructField("CODE", StringType()),
    StructField("DESCRIPTION", StringType()),
    StructField("BASE_ENCOUNTER_COST", DoubleType()),
    StructField("TOTAL_CLAIM_COST", DoubleType()),
    StructField("PAYER_COVERAGE", DoubleType()),
    StructField("REASONCODE", StringType()),
    StructField("REASONDESCRIPTION", StringType()),
])

logger.info(f"Reading {encounters_file}...")
df = spark.read \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
    .schema(encounters_schema) \
    .csv(encounters_file)

# CLEANING

pre_clean_record_count = df.count()
logger.info(f"Original record count: {pre_clean_record_count}")

# Clean string columns - convert empty strings and "null" to None, and trim whitespace
string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
for column in string_columns:
    df = df.withColumn(
        column, 
        when((col(column) == "") | (col(column) == "null"), None).otherwise(trim(col(column)))
    )

# Handle timestamp columns separately if they need conversion
# The schema should handle this automatically with timestampFormat option

# Remove rows with NULL in required fields
logger.info("Per the encounters data dictionary, removing records with null values in required fields...")
required_fields = ["Id", "START", "PATIENT", "ORGANIZATION", "PROVIDER", "PAYER", 
                  "ENCOUNTERCLASS", "CODE", "DESCRIPTION", "BASE_ENCOUNTER_COST", 
                  "TOTAL_CLAIM_COST", "PAYER_COVERAGE"]

for field in required_fields:
    df = df.filter(col(field).isNotNull())

null_records_removed = pre_clean_record_count - df.count()
logger.info(f"Removed {null_records_removed} records with null values in required fields")
pre_clean_record_count = df.count()

# Data validation and cleaning
logger.info("Performing data validation and cleaning...")


df = df.dropDuplicates(["Id"]) \
    .filter(col("ENCOUNTERCLASS").isin(["ambulatory", "emergency", "inpatient", "wellness", "urgentcare"])) \
    .filter(col("BASE_ENCOUNTER_COST") >= 0) \
    .filter(col("TOTAL_CLAIM_COST") >= 0) \
    .filter(col("PAYER_COVERAGE") >= 0) \
    .filter(col("STOP").isNull() | (col("STOP") >= col("START"))) \
    .filter(col("TOTAL_CLAIM_COST") >= col("BASE_ENCOUNTER_COST")) \
    .filter(col("PAYER_COVERAGE") <= col("TOTAL_CLAIM_COST"))




malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"Removed {malformed_records_removed} records not conforming to the specifications")

final_record_count = df.count()
logger.info(f"Final record count: {final_record_count}")

# OUTPUT

logger.info(f"Saving cleaned data to {output}...")
df.write \
    .mode("overwrite") \
    .parquet(output)

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")
logger.info(f"Data cleaning summary:")
logger.info(f"  - Original records: {pre_clean_record_count + null_records_removed}")
logger.info(f"  - Null records removed: {null_records_removed}")
logger.info(f"  - Malformed records removed: {malformed_records_removed}")
logger.info(f"  - Final records: {final_record_count}")

spark.stop()
