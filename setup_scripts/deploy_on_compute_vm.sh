#!/bin/bash
github_project_name="synthea-pipeline"
github_repo_name="https://github.com/MichaelSalata/synthea-pipeline.git"
git_branch="main"

sudo snap install docker
sudo groupadd -f docker
sudo usermod -aG docker $(whoami)

echo "Waiting for Docker socket to be available..."
while [ ! -S /var/run/docker.sock ]; do sleep 1; done

sudo chown root:docker /var/run/docker.sock
sudo chmod 660 /var/run/docker.sock

newgrp docker <<EOF
if [ ! -d "${github_project_name}" ]; then
  git clone --branch ${git_branch} ${github_repo_name}
fi

cp -f ./terraform.tfvars ./${github_project_name}/terraform/terraform.tfvars
cp -f ./.env ./${github_project_name}/airflow-gcp/.env
bash update_vm_env.sh

echo "downloading Synthea sample data..."
wget -O synthea_sample_data.zip https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip

echo "creating example_data directory and extracting files..."
mkdir -p ./${github_project_name}/airflow-gcp/example_data/
unzip -o synthea_sample_data.zip -d ./${github_project_name}/airflow-gcp/example_data/
rm synthea_sample_data.zip

cd ./${github_project_name}/airflow-gcp
mkdir ./logs ./plugins ./config
DOCKER_BUILDKIT=1 docker compose build
docker compose up -d airflow-init && docker compose up
EOF