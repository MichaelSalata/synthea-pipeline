terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "bucket-name" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_storage_bucket" "dataproc-temp-bucket" {
  name          = "dataproc_temp_bucket_${var.project}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

}

resource "google_storage_bucket" "dataproc-staging-bucket" {
  name          = "dataproc_staging_bucket_${var.project}"
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

}

resource "google_dataproc_cluster" "dataproc-cluster" {
  name     = var.dataproc_cluster_name
  location = var.region
  graceful_decommission_timeout = "120s"


  cluster_config {
    staging_bucket = "dataproc_staging_bucket_${var.project}"
    temp_bucket = "dataproc_temp_bucket_${var.project}"
    
#    gce_cluster_config {
#      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
#      service_account = "terraform@${var.project}.iam.gserviceaccount.com"
#      service_account_scopes = [
#        "cloud-platform"
#      ]
#    }

    master_config {
      num_instances = 1
    }

    worker_config {
      num_instances    = 7
    }
  }
}

resource "google_bigquery_dataset" "dataset-name" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
}


module "vpc" {
  source = "./vpc"

  vpc_network_name = var.vpc_network_name
  firewall_name    = var.firewall_name
}


module "compute" {
  source = "./compute"

  instance_name        = var.instance_name
  machine_type         = var.machine_type
  zone                 = var.zone
  image                = var.image
  network              = module.vpc.vpc_id
  ssh_user             = var.ssh_user
  public_ssh_key_path  = var.public_ssh_key_path
  private_ssh_key_path = var.private_ssh_key_path
  target_bucket        = var.gcs_bucket_name
  credentials          = var.credentials
  compute_disk_size_GB = var.compute_disk_size_GB
}

