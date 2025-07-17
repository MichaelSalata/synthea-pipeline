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


# Apply filters

# NOTE: col(ENCOUNTERCLASS) seems to include these values too ['outpatient', 'home', 'hospice', 'ambulatory', 'snf']
acceptable_encounter_classes = ["ambulatory", "emergency", "inpatient", "wellness", "urgentcare"]
filters = {
    "Id_isUnique": lambda df: df.dropDuplicates(["Id"]),
    "encounter_class_set": lambda df: df.filter(col("ENCOUNTERCLASS").isin(acceptable_encounter_classes)),
    "base_cost_nonNeg": lambda df: df.filter(col("BASE_ENCOUNTER_COST") >= 0),
    "total_cost_nonNeg": lambda df: df.filter(col("TOTAL_CLAIM_COST") >= 0),
    "payer_coverage_nonNeg": lambda df: df.filter(col("PAYER_COVERAGE") >= 0),
    "date_range_stop_after_start": lambda df: df.filter(col("STOP").isNull() | (col("STOP") >= col("START"))),
    "base_cost_<=_total": lambda df: df.filter(col("BASE_ENCOUNTER_COST") <= col("TOTAL_CLAIM_COST")),
    "payer_amnt_<=_total": lambda df: df.filter(col("PAYER_COVERAGE") <= col("TOTAL_CLAIM_COST")),
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

logger.info(f"Saving cleaned data to {output}...")
df_result.write.write.format('bigquery') \
  .mode('overwrite') \
  .option('table', output) \
  .save()

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")
logger.info(f"Data cleaning summary:")
logger.info(f"  - Original records: {pre_clean_record_count + null_records_removed}")
logger.info(f"  - Null records removed: {null_records_removed}")
logger.info(f"  - Malformed records removed: {malformed_records_removed}")
logger.info(f"  - Final records: {final_record_count}")

spark.stop()
