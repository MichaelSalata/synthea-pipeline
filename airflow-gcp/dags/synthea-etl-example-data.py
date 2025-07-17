import os
import json
import subprocess
import logging
import glob
from datetime import datetime

from google.cloud import bigquery

from airflow.decorators import dag, task, task_group  # Add task_group to the import
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
    for name in files:
        glob_found = glob.glob(f"./example_data/{name}*.csv")
        matched_files.extend(glob_found)
        task_logger.info(f"Found {glob_found} files matching {name}*.csv")

    return matched_files

@task
def upload_to_gcs(local_filepath: str, gcs_filepath: str=None):
    filename = os.path.basename(local_filepath)
    gcp_blob = gcs_filepath if gcs_filepath else filename

    gcs_hook = GCSHook()

    if gcs_hook.exists(bucket_name=GCP_GCS_BUCKET, object_name=gcp_blob):
        task_logger.warning(f"{filename} already exists in at {gcp_blob} on GCS. Skipping upload.")
        return gcp_blob
    
    task_logger.info(f"Uploading {filename} to {gcp_blob}...")
    gcs_hook.upload(bucket_name=GCP_GCS_BUCKET, object_name=gcp_blob, filename=local_filepath)
    task_logger.info(f"Upload successful to {gcp_blob}")
    return gcp_blob


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


# TODO: create task to start the Dataproc cluster

# TODO: create task to submit Spark jobs to the Dataproc cluster

# TODO: create task to stop the Dataproc cluster


default_args = {
    "owner": "MSalata",             # default: airflow
    "depends_on_past": False,       # default: False
    "retries": 0,                   # default: 0
}

@dag(
    dag_id="synthea-etl-example-data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),  # Add this line
    schedule="@monthly",  # default: None
    catchup=False,
    tags=['synthea', 'etl'],
)
def synthea_etl_example_data():
    synthea_tables = [
        "patients",
        "medications",
        "encounters",
        "organizations",
        # "payers",
        # "providers",
    ]


    @task_group
    def ETL_synthea_data(local_filepath: str):
        # upload data to GCS -> output glob location
        csv_in_gcs = upload_to_gcs(local_filepath=local_filepath)

        # TODO: submit jobs to DataProc Cluster to process csv_in_gcs with Spark
            # Spark job should output to BigQuery tables (potentially using URI?)


    ETL_synthea_data.expand(local_filepath=find_example_data(synthea_tables))

    # create_dataproc_cluster >> ETL_synthea_data >> [delete_cluster, run_dbt()]
    

synthea_dag = synthea_etl_example_data()
