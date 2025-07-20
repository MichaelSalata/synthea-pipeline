# Overview
This is an ETL data pipeline for Synthea healthcare data. The dataset was synthetically generated to mimic modern, realistic healthcare data formats. This pipeline aims to create a comprehensive medications-centered dataset to enable powerful analytics surrounding patient prescriptions.

RESOURCE: [SyntheticMass](https://synthea.mitre.org/)

## What it does
### **Creates an ETL Pipeline** for the Synthea Medical Data
1. **starts GCP cloud resources** (Google Cloud VM, Google Cloud Storage Buckets, Google Dataproc, BigQuery Dataset)
2. **sets up Dockerized Airflow & dbt** on the Cloud VM
3. **uploads example data** to the Google Cloud Bucket
4. **Processes example_data** csv's with **Spark** (GCP Dataproc)
5. **Models a BigQuery fact table** centering around user Prescriptions

## Use Cases
- **Medication Cost Analysis**: Track prescription costs across different demographics, locations, and time periods
- **Geographic Healthcare Insights**: Analyze medication prescribing patterns by city, state, and healthcare organization
- **Healthcare Provider Performance**: Compare organization revenue, utilization rates, and patient coverage across regions
- **Healthcare Economics**: Analyze payer coverage patterns and total healthcare costs per patient
- **Temporal Health Trends**: Track seasonal medication patterns and prescription volume changes over time

## [Looker Studio Geomap Dashboard](https://lookerstudio.google.com/s/gSERWFPhl1A)
[![Synthea Prescription Heatmap Dashboard](https://github.com/MichaelSalata/synthea-pipeline/blob/main/imgs/Medication_Code_stats.png)](https://lookerstudio.google.com/s/gSERWFPhl1A)

## [Looker Studio Statistics Dashboard](https://lookerstudio.google.com/s/lv6t7ghdid4)
[![Medication Code Statistics Dashboard](https://github.com/MichaelSalata/synthea-pipeline/blob/main/imgs/Medication_Code_stats.png)](https://lookerstudio.google.com/s/lv6t7ghdid4)

## Technologies Used
- **Terraform** provisions Cloud Resources with Infrastructure-as-Cloud (IaC).
- **Docker** encapsulates the pipeline ensuring portability.
- **Apache Airflow** *orchestrates and schedules* download, reformatting, upload, database transfer and SQL transformation.
- **Python** to **connect and download** from the Fitbit API and **reformat** the downloaded json files to parquet
- **PostgreSQL** provides Airflow a **database to store workflow metadata** about DAGs, tasks, runs, and other elements
- **Spark** cleans & processes the csv data. Run with GCP Dataproc. 
- **Google BigQuery** to **process data analytics**. **Table partitioning is done in the dbt staging process**
- **dbt (Data Build Tool)** injects SQL **data transformations** into BigQuery. Keeping SQL externally allows version control better to better maintain SQL code.


# HOW TO Setup and Deploy on Google Cloud
## 1. Requirements
[Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform),  [Google Cloud Platform Project](https://console.cloud.google.com/),  [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)

## 2. Setup a Service Account for a Google Cloud Project
- create a service account and download a .json key file
	1. GCP Dashboard -> IAM & Admin > Service accounts > Create service account
	2. set a name & Leave all other fields with default values -> Create and continue
	3. Grant the Viewer role (Basic > Viewer) -> Continue -> Done
	4. 3 dots below Actions -> Manage keys -> Add key -> Create new key -> JSON -> Create
		1. **Alternatively**, go to your [Service Accnt List](https://console.cloud.google.com/iam-admin/serviceaccounts)
		2. Select 3 dots under **Actions** -> **Manage details** -> Keys -> Add key -> Create new key -> JSON -> Create
	5. be sure to add this keys path to your `./terraform/terraform.tfvars`

- **Add Cloud Storage & BigQuery permissions** to your Service Account
	1. find your service account at [IAM Cloud UI](https://console.cloud.google.com/iam-admin/iam) 
	2. use `+Add another role` to add these roles
		- **Storage** Admin
		- **Storage Object** Admin
		- **BigQuery** Admin
        - **Dataproc** Administrator
		- Service Account User
		- Viewer
	3. Enable the [IAM API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
	4. Enable the [IAM Service Account Credentials API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)
	5. Enable the [Cloud Dataproc API](https://console.cloud.google.com/apis/library/dataproc.googleapis.com)
	6. ~~Enable the [Cloud Dataproc Control API](https://console.cloud.google.com/apis/library/dataproc.googleapis.com)~~
- **Add Compute VM permissions** to your Service Account
	1. find your service account at [IAM Cloud UI](https://console.cloud.google.com/iam-admin/iam) 
	2. use `+Add another role` to add these roles
		- **Compute Instance** Admin
		- **Compute Network** Admin
		- **Compute Security** Admin
	3. Enable the [Compute Engine API](https://console.cloud.google.com/apis/library/compute.googleapis.com)

## 3. Prepare Terraform to Launch The Project
### 3-a. Clone this Project Locally
```bash
git clone https://github.com/MichaelSalata/synthea-pipeline.git
```
### 3-b. Create your SSH key
```bash
ssh-keygen -t rsa -b 2048 -C "your_ssh_username@example.com"
```
### 3-c. Fill Out `terraform/terraform.tfvars`
copy `terraform.tfvars-example` into `terraform.tfvars` and fill out your own details
```bash
cp ./terraform/terraform.tfvars-example ./terraform/terraform.tfvars
```
**NOTE**: pick a **unique** `gcs_bucket_name` like  `projectName-fitbit-bucket`
```
credentials          = "/path/to/service_credentials.json"
project              = "google_project_name"
gcs_bucket_name      = "UNIQUE-google-bucket-name"
ssh_user = "your_ssh_username_WITHOUT@example.com"
public_ssh_key_path = "~/path/to/id_rsa.pub"
private_ssh_key_path = "~/path/to/id_rsa"
```
## 4. Launch with Terraform
```bash
cd ./terraform
terraform init
terraform apply
```
## 5. Launch the DAG from Airflow's Webserver
**OPTION 1**:
- run `bash ./setup_scripts/visit_8080_on_vm.sh`

**OPTION 2**:
- get your Compute Instance's **External IP** in [your Google VM instances](https://console.cloud.google.com/compute/instances)
- visit **External IP**:8080
- choose and run the appropriate DAG

## 6. Close Down Resources
1. `Ctrl+C` will stop Terraform running
2. Run these commands to destroy cloud resources Terraform provisioned
```bash
cd ./terraform
terraform destroy
```

# Future Goals
- [x] implement BigQuery Partitioning and Clustering
