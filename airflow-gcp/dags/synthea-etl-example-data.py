import os
import json
import subprocess
import logging
import glob

from google.cloud import bigquery

from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.providers.google.cloud.hooks.gcs import GCSHook


PROJECT_ID = str(os.environ.get("GCP_PROJECT_ID"))
GCP_GCS_BUCKET = str(os.environ.get("GCP_GCS_BUCKET", f"{PROJECT_ID}-fitbit-bucket"))
BIGQUERY_DATASET = str(os.environ.get("BIGQUERY_DATASET", "fitbit_dataset"))

airflow_path = os.environ.get("AIRFLOW_HOME")
DBT_IS_TEST_RUN = os.environ.get("IS_DEV_ENV", True)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
task_logger = logging.getLogger(__name__)



@task
def find_example_data(files: list[str]):
    matched_files = []
    for b in files:
        matched_files.extend(glob.glob(f"./example_data/{b}*.csv"))

    return matched_files


@task
def upload_to_gcs(filename: str, gcs_filepath: str):
    gcp_blob = f"{gcs_filepath}/{filename}"
    task_logger.info(f"Uploading {filename} to {gcp_blob}...")
    gcs_hook = GCSHook()
    gcs_hook.upload(bucket_name=GCP_GCS_BUCKET, object_name=gcp_blob, filename=file_path_map["filepath"])
    task_logger.info(f"Upload successful to {gcp_blob}")
    return gcp_blob


@task
def setup_bq_ext_tables(tablename: str):



# validate and join tables with dbt
@task
def run_dbt():
    dbt_command = " && ".join([
        f"cd {airflow_path}/dbt_resources",
        "dbt deps",
        "dbt build --vars '{is_test_run: " + str(DBT_IS_TEST_RUN) + "}'"
        ])
    try:
        task_logger.info("Executing:", dbt_command)
        subprocess.run(dbt_command, shell=True, check=True, text=True)
        task_logger.info("DBT commands ran successfully.")
    except subprocess.CalledProcessError as e:
        task_logger.error(f"DBT command failed: {e}")
        raise e








default_args = {
    "owner": "MSalata",             # default: airflow
    "depends_on_past": False,       # default: False
    "retries": 0,                   # default: 0
}

@dag(
    dag_id="synthea-etl-example-data",
    default_args=default_args,
    schedule=@monthly,  # default: None
    catchup=False,
    tags=['synthea', 'etl'],
)
def synthea_etl_example_data():
    # the only fitbit data currently supported is both:
        # keyword mapped in FitbitHook.py plugin class
        # AND
        # keyword mapped in fitbit_json_to_parquet.py  flatten_fitbit_json func

    synthea_tables = [
        "patients",
        "medications",
        "encounters",
        "organizations",
        # "payers",
        # "providers",
    ]



    @task_group
    def ETL_synthea_data(filename: str):
        # upload data to GCS -> output glob locations
        
        # append etl date to filename and upload to GCS
        upload_to_gcs

        # process data with Spark
            # have Spark jobs output to BigQuery tables


    csvs_in_gcs = ETL_synthea_data.expand(find_example_data(            ))


    # create BigQuery external tables for parquet files in GCS
    setup_bq_ext_tables = parquets_to_bq_table.expand(endpoint_id=BQ_TABLES)


    csvs_in_gcs >> setup_bq_ext_tables.expand(endpoint_id=BQ_TABLES) >> run_dbt()
    

synthea_dag = synthea_etl_example_data()
