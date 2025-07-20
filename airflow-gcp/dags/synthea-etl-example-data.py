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
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook


PROJECT_ID = str(os.environ.get("GCP_PROJECT_ID"))
GCP_GCS_BUCKET = str(os.environ.get("GCP_GCS_BUCKET"))
BIGQUERY_DATASET = str(os.environ.get("BIGQUERY_DATASET"))
DATAPROC_CLUSTER_NAME = str(os.environ.get("DATAPROC_CLUSTER_NAME"))
DATAPROC_REGION = str(os.environ.get("DATAPROC_REGION"))

airflow_path = os.environ.get("AIRFLOW_HOME")
DBT_IS_TEST_RUN = os.environ.get("DBT_IS_TEST_RUN", True)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
task_logger = logging.getLogger(__name__)

@task
def find_examp_csv_file(table_name: str):
    csv_files = list(glob.iglob(f"./example_data/{table_name}.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found matching ./example_data/{table_name}.csv")
    csv = csv_files[0]  # Take the first matching file
    task_logger.info(f"Found {csv} matching {table_name}*.csv")
    return csv

@task
def upload_to_gcs(local_filepath: str, gcs_filepath: str=None, force_overwrite: bool=False):
    filename = os.path.basename(local_filepath)
    gcp_blob = gcs_filepath if gcs_filepath else filename

    gcs_hook = GCSHook()

    if gcs_hook.exists(bucket_name=GCP_GCS_BUCKET, object_name=gcp_blob):
        if force_overwrite:
            task_logger.info(f"{filename} already exists at {gcp_blob} on GCS. Overwriting due to force_overwrite=True.")
        else:
            task_logger.warning(f"{filename} already exists at {gcp_blob} on GCS. Skipping upload.")
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
def start_dataproc_cluster():
    # validate required environment variables
    if not PROJECT_ID or PROJECT_ID == "None":
        raise ValueError("GCP_PROJECT_ID environment variable is not set")
    if not DATAPROC_CLUSTER_NAME or DATAPROC_CLUSTER_NAME == "None":
        raise ValueError("DATAPROC_CLUSTER_NAME environment variable is not set")
    if not DATAPROC_REGION or DATAPROC_REGION == "None":
        raise ValueError("DATAPROC_REGION environment variable is not set")
    
    task_logger.info(f"Starting/checking cluster: {DATAPROC_CLUSTER_NAME} in region: {DATAPROC_REGION} for project: {PROJECT_ID}")
    
    dataproc_hook = DataprocHook()
    
    try:
        cluster = dataproc_hook.get_cluster(
            project_id=PROJECT_ID,
            region=DATAPROC_REGION,
            cluster_name=DATAPROC_CLUSTER_NAME
        )
        
        cluster_status = cluster.status.state
        task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} exists with status: {cluster_status}")
        
        if cluster_status.name == "RUNNING":
            task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} is already running")
        elif cluster_status.name == "STOPPED":
            task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} is stopped. Starting it now...")
            operation = dataproc_hook.start_cluster(
                project_id=PROJECT_ID,
                region=DATAPROC_REGION,
                cluster_name=DATAPROC_CLUSTER_NAME
            )
            dataproc_hook.wait_for_operation(operation)
            task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} has been started successfully")
        elif cluster_status.name in ["STARTING", "STOPPING"]:
            task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} is in transition state: {cluster_status.name}")
            
            import time
            time.sleep(30)
            cluster = dataproc_hook.get_cluster(
                project_id=PROJECT_ID,
                region=DATAPROC_REGION,
                cluster_name=DATAPROC_CLUSTER_NAME
            )
            task_logger.info(f"Cluster {DATAPROC_CLUSTER_NAME} status after waiting: {cluster.status.state.name}")
        else:
            task_logger.warning(f"Cluster {DATAPROC_CLUSTER_NAME} is in unexpected state: {cluster_status.name}")
            
    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg or "not found" in error_msg.lower():
            task_logger.error(f"Cluster {DATAPROC_CLUSTER_NAME} does not exist in project {PROJECT_ID}, region {DATAPROC_REGION}")
            task_logger.error("Please create the cluster first using Terraform or the Google Cloud Console")
            raise ValueError(f"Dataproc cluster '{DATAPROC_CLUSTER_NAME}' not found")
        else:
            task_logger.error(f"Error managing cluster {DATAPROC_CLUSTER_NAME}: {e}")
            raise e

    return DATAPROC_CLUSTER_NAME


@task
def submit_spark_job(table_name: str, spark_gcs_path: str, csv_gcs_path: str):
    dataproc_hook = DataprocHook()
    
    bq_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_raw"
    
    spark_file_uri = f"gs://{GCP_GCS_BUCKET}/{spark_gcs_path}"
    csv_file_uri = f"gs://{GCP_GCS_BUCKET}/{csv_gcs_path}"
    
    job_id = f"synthea-{table_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    # TODO: test if setting spark.conf.temporaryGcsBucket doesn't do anything
    job_config = {
        "reference": {"job_id": job_id},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": spark_file_uri,
            "args": [
                "--csv", csv_file_uri,
                "--output", bq_table,
                "--bq_transfer_bucket", f"dataproc_temp_bucket_{PROJECT_ID}",
            ],
            "properties": {
                "spark.conf.temporaryGcsBucket": f"dataproc_temp_bucket_{PROJECT_ID}"
            }
        },
    }
    
    task_logger.info(f"Submitting Spark job {job_id} for {bq_table} in BQ")
    task_logger.info(f"Spark script: {spark_file_uri}")
    task_logger.info(f"Input CSV: {csv_file_uri}")
    task_logger.info(f"Output BigQuery table: {bq_table}")
    
    try:
        operation = dataproc_hook.submit_job(
            project_id=PROJECT_ID,
            region=DATAPROC_REGION,
            job=job_config
        )
        
        task_logger.info(f"Spark job {job_id} submitted successfully")
        
        dataproc_hook.wait_for_job(
            job_id=job_id, 
            region=DATAPROC_REGION,
            project_id=PROJECT_ID,
            timeout=600
        )
        
        task_logger.info(f"Spark job {job_id} completed successfully")
        return f"Job {job_id} completed successfully for table {table_name}"
        
    except Exception as e:
        task_logger.error(f"Spark job {job_id} failed: {e}")
        raise e

@task
def stop_idle_dataproc_cluster(cluster_name: str):
    dataproc_hook = DataprocHook()
    
    try:
        cluster = dataproc_hook.get_cluster(
            project_id=PROJECT_ID,
            region=DATAPROC_REGION,
            cluster_name=cluster_name
        )
        cluster_status = cluster.status.state
        
        if cluster_status.name != "RUNNING":
            task_logger.info(f"Cluster {cluster_name} is not running (status: {cluster_status.name}). No need to stop.")
            return f"Cluster {cluster_name} was already in state: {cluster_status.name}"
        
        task_logger.info(f"Cluster {cluster_name} is running. Checking for active jobs...")
        
        job_client = dataproc_hook.get_job_client(region=DATAPROC_REGION)
        
        # List active jobs for this cluster using correct API parameters
        request = {
            "project_id": PROJECT_ID,
            "region": DATAPROC_REGION,
            "cluster_name": cluster_name,
            "job_state_matcher": "ACTIVE"  # This matches PENDING, RUNNING, CANCEL_PENDING
        }
        
        active_jobs = list(job_client.list_jobs(request=request))
        
        if active_jobs:
            task_logger.warning(f"Found {len(active_jobs)} active jobs on cluster {cluster_name}:")
            task_logger.warning(f"Not stopping cluster {cluster_name} due to active jobs. Consider stopping it manually later.")
            return f"Cluster {cluster_name} not stopped - {len(active_jobs)} active jobs found"
        
        task_logger.info(f"No active jobs found on cluster {cluster_name}. Proceeding to stop the cluster...")
        

        # NOTE: graceful_decommission_timeout = "120s" in Terraform means:
        # - YARN will gracefully decommission nodes
        # - Wait up to 120 seconds for running jobs to complete
        # - After timeout, forcefully stop remaining jobs
        operation = dataproc_hook.stop_cluster(
            project_id=PROJECT_ID,
            region=DATAPROC_REGION,
            cluster_name=cluster_name
        )
        
        task_logger.info(f"Stop operation initiated for cluster {cluster_name}")
        task_logger.info("Terraform's graceful_decommission_timeout=120s will handle graceful shutdown")
        
        # Optionally wait for the stop operation to complete
        # dataproc_hook.wait_for_operation(operation)
        
        return f"Cluster {cluster_name} stop operation initiated successfully"
        
    except Exception as e:
        task_logger.error(f"Error stopping cluster {cluster_name}: {e}")
        raise e


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
    table_spark_jobs = [
        {"table":"patients",    "local_spark":os.path.join(airflow_path, "spark", "patients_dataproc_to_bq.py")},
        {"table":"medications", "local_spark":os.path.join(airflow_path, "spark", "medications_dataproc_to_bq.py")},
        {"table":"encounters",  "local_spark":os.path.join(airflow_path, "spark", "encounters_dataproc_to_bq.py")},
        {"table":"organizations", "local_spark":os.path.join(airflow_path, "spark", "organizations_dataproc_to_bq.py")},
        # {"table":"payers", "local_spark":os.path.join(airflow_path, "spark", "payers_dataproc_to_bq.py")},
        # {"table":"providers", "local_spark":os.path.join(airflow_path, "spark", "providers_dataproc_to_bq.py")},
    ]

    @task_group
    def ETL_synthea_data(table: str, local_spark: str):
        spark_in_gcs = upload_to_gcs(local_filepath=local_spark, force_overwrite=True)

        # later replace examples with API call or data download (NOTE: stream download/upload chunks to GCS not to run out of memory)
        # -----------------
        csv_file = find_examp_csv_file(table_name=table)
        csv_in_gcs = upload_to_gcs(local_filepath=csv_file)
        # -----------------

        spark_job_result = submit_spark_job(
            table_name=table,
            spark_gcs_path=spark_in_gcs,
            csv_gcs_path=csv_in_gcs
        )
        
        return spark_job_result

    gcp_dataproc_cluster = start_dataproc_cluster()
    etl_tasks = ETL_synthea_data.expand_kwargs(table_spark_jobs)
    cluster_stop = stop_idle_dataproc_cluster(gcp_dataproc_cluster)
    dbt_task = run_dbt()
    
    gcp_dataproc_cluster >> etl_tasks >> [cluster_stop, dbt_task] # >> remove temporary files if needed

    return gcp_dataproc_cluster
    
synthea_dag = synthea_etl_example_data()
