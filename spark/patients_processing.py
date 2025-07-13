import argparse
import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, trim, to_date


parser = argparse.ArgumentParser()
parser.add_argument('--patients_file', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

patients_file = args.patients_file
output = args.output


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('patients_data_processing') \
    .getOrCreate()


patients_schema = StructType([
    StructField("Id", StringType()),
    StructField("BirthDate", DateType()),
    StructField("DeathDate", DateType()),
    StructField("SSN", StringType()),
    StructField("Drivers", StringType()),
    StructField("Passport", StringType()),
    StructField("Prefix", StringType()),
    StructField("First", StringType()),
    StructField("Middle", StringType()),
    StructField("Last", StringType()),
    StructField("Suffix", StringType()),
    StructField("Maiden", StringType()),
    StructField("Marital", StringType()),
    StructField("Race", StringType()),
    StructField("Ethnicity", StringType()),
    StructField("Gender", StringType()),
    StructField("BirthPlace", StringType()),
    StructField("Address", StringType()),
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("County", StringType()),
    StructField("FIPS_County_Code", StringType()),
    StructField("Zip", IntegerType()),
    StructField("Lat", DoubleType()),
    StructField("Lon", DoubleType()),
    StructField("Healthcare_Expenses", DoubleType()),
    StructField("Healthcare_Coverage", DoubleType()),
    StructField("Income", DoubleType()),
])

logger.info("Reading {patients_file}...")
df = spark.read \
    .option("header", "true") \
    .option("dateFormat", "YYYY-MM-DD") \
    .schema(patients_schema) \
    .csv(patients_file)


pre_clean_record_count = df.count()
logger.info(f"Original record count: {pre_clean_record_count}")

string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
for column in string_columns:
    df = df.withColumn(
        column, 
        when((col(column) == "") | (col(column) == "null"), None).otherwise(trim(col(column)))
    )

# remove rows with NULL in a required field
logger.info("per the schema defined at https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary#patients\nnull values for required fields are not allowed")
required_fields = ["Id", "BirthDate", "SSN", "First", "Last", "Race", "Ethnicity", 
                   "Gender", "BirthPlace", "Address", "City", "State", 
                   "Healthcare_Expenses", "Healthcare_Coverage", "Income"]
for field in required_fields:
    df = df.filter(col(field).isNotNull())


null_records_removed = pre_clean_record_count - df.count()
logger.info(f"removed {null_records_removed} records with null values in required fields")
pre_clean_record_count = df.count()


logger.info("NOTE: per the schema defined at https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary#patients\n" \
    "Gender must be M or F, Marital must be M or S" \
    "Healthcare_Expenses, Healthcare_Coverage, and Income must be >= 0")
df = df.dropDuplicates(["Id"]) \
    .filter(col("Gender").isin(["M", "F"])) \
    .filter(col("Marital").isin(["M", "S"])) \
    .filter(col("Healthcare_Expenses") >= 0) \
    .filter(col("Healthcare_Coverage") >= 0) \
    .filter(col("Income") >= 0)
# TODO: Check for unrealistic birth dates

malformed_records_removed = pre_clean_record_count - df.count()
logger.info(f"removed {malformed_records_removed} records not conforming to the specifications")

logger.info("Final schema:")
df.printSchema()

# Save as parquet
logger.info(f"Saving cleaned data to {output}...")
df.write \
    .mode("overwrite") \
    .parquet(output)

logger.info("Data processing completed successfully!")
logger.info(f"Cleaned data saved to: {output}")

df.printSchema()

# TODO: save the summary report to a file - include malformed_records_removed and null_records_removed

# Stop Spark session
spark.stop()