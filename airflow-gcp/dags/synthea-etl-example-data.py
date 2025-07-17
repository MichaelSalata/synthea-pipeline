import os
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
def find_example_csv(files: list[str]):
    matched_files = []
    for name in files:
        glob_found = 
        matched_files.extend(glob_found)
        task_logger.info(f"Found {glob_found} files matching {name}*.csv")

    return 

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
@task
def start_cluster():
    # placehoder
    return None

# TODO: create task to stop the Dataproc cluster
@task
def stop_cluster():
    # placehoder
    return None


default_args = {
    "owner": "MSalata",             # default: airflow
    "depends_on_past": False,       # default: False
    "retries": 0,                   # default: 0
}

@dag(
    dag_id="synthea-etl-example-data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=['synthea', 'etl'],
)
def synthea_etl_example_data():
    table_spark_jobs = {
        {"table":"patients",    "local_spark":os.path.join(airflow_path, "spark", "patients_dataproc_to_bq.py")},
        {"table":"medications", "local_spark":os.path.join(airflow_path, "spark", "medications_dataproc_to_bq.py")},
        {"table":"encounters",  "local_spark":os.path.join(airflow_path, "spark", "encounters_dataproc_to_bq.py")},
        {"table":"organizations", "local_spark":os.path.join(airflow_path, "spark", "organizations_dataproc_to_bq.py")},
        # {"table":"payers", "local_spark":os.path.join(airflow_path, "spark", "payers_dataproc_to_bq.py")},
        # {"table":"providers", "local_spark":os.path.join(airflow_path, "spark", "providers_dataproc_to_bq.py")},
    }


    @task_group
    def ETL_synthea_data(table: str, local_spark: str):

        spark_in_gcs = upload_to_gcs(local_filepath=local_spark)

        # replace examples with API call or data download and stream upload chunks to GCS
        csv = glob.glob(f"./example_data/{table}*.csv") 
        csv_in_gcs = upload_to_gcs(local_filepath=csv)

        # TODO: submit spark_in_gcs jobs to DataProc Cluster to process csv_in_gcs with Spark
            # Spark job should output/overwrite tables to BigQuery tables (potentially using URI?)

        # clean up 

    # TODO: setup task dependancies below
    # potentially pass in the cluster as a .partial
    start_cluster >> ETL_synthea_data.expand_kwargs(table_spark_jobs) >> [stop_cluster, run_dbt()]
    

synthea_dag = synthea_etl_example_data()
