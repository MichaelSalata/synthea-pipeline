resource "google_compute_instance" "default" {
  name         = var.instance_name
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = var.image
    }
  }

  network_interface {
    network = var.network
    access_config {}
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.public_ssh_key_path)}",
    VAR_1 = var.target_bucket,
    VAR_2 = "synthea-pipe_dw"    
  }

  connection {
        type        = "ssh"
        user        = var.ssh_user
        private_key = file(var.private_ssh_key_path)
        host        = google_compute_instance.default.network_interface[0].access_config[0].nat_ip
      }
      
  provisioner "file" {
        source      = "../setup_scripts/deploy_on_compute_vm.sh"
        destination = "/home/${var.ssh_user}/deploy_on_compute_vm.sh"
      }
      
      
  provisioner "file" {
        source      = "../setup_scripts/update_vm_env.sh"
        destination = "/home/${var.ssh_user}/update_vm_env.sh"
      }


  provisioner "file" {
        source      = "./terraform.tfvars"
        destination = "/home/${var.ssh_user}/terraform.tfvars"
      }

  provisioner "file" {
        source      = "../airflow-gcp/.env"
        destination = "/home/${var.ssh_user}/.env"
      }

  provisioner "file" {
        source      = "${var.credentials}"
        destination = "/home/${var.ssh_user}/google_credentials.json"
      }

  provisioner "remote-exec" {
        inline = [
          "chmod +x /home/${var.ssh_user}/deploy_on_compute_vm.sh",
          "/home/${var.ssh_user}/deploy_on_compute_vm.sh"
          ]
      }
      
}

# Requires the ability to impersonate itself
# gcloud iam service-accounts add-iam-policy-binding gcp-synthea-pipe-ctrl@gcp-online-test.iam.gserviceaccount.com \
#   --member="serviceAccount:gcp-synthea-pipe-ctrl@gcp-online-test.iam.gserviceaccount.com" \
#   --role="roles/iam.serviceAccountUser" \
#   --project=gcp-online-test

resource "google_dataproc_cluster" "dataproc-cluster" {
  name     = var.dataproc_cluster_name
  region   = var.region
  graceful_decommission_timeout = "120s"

  cluster_config {
    staging_bucket = "dataproc_staging_bucket_${var.project}"
    temp_bucket = "dataproc_temp_bucket_${var.project}"
    gce_cluster_config {
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = "gcp-synthea-pipe-ctrl@gcp-online-test.iam.gserviceaccount.com"
      service_account_scopes = [
        "cloud-platform"
      ]
    }
  }
}
