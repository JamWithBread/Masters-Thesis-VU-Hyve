#! /bin/bash

### Install environment packages and dependencies ###

apt-get update
apt-get install -y python3-pip
sudo apt install -y ca-certificates-java
sudo apt install -y openjdk-17-jdk
gsutil cp gs://hvd-pgp-genomics-files/VM_startup/requirements.txt .
gsutil cp gs://hvd-pgp-genomics-files/VM_startup/install-bioinformatics-tools.sh .
pip3 install -r requirements.txt
source install-bioinformatics-tools.sh

### Copy Python scripts to working directory ###

mkdir -p /home/jeren/pipeline-scripts
gsutil cp gs://hvd-pgp-genomics-files/pipeline-scripts/keys/service-account-key.json /home/jeren/pipeline-scripts/keys/service-account-key.json 
gsutil -m cp -r gs://hvd-pgp-genomics-files/pipeline-scripts/* /home/jeren/pipeline-scripts/ 

### Get instance group metadata for file distribution ###

mkdir /home/metadata

ZONE=europe-west4-a
INSTANCE_GROUP=genomics-workload-group-1

gcloud compute instance-groups managed list-instances --zone=$ZONE $INSTANCE_GROUP --format=yaml > /home/metadata/instance-group-info.yaml
curl -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/id" > /home/metadata/instance-id.txt
curl -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/hostname" > /home/metadata/instance-hostname.txt

### Assign subset of files to be processed in this VM ###

gsutil cp gs://hvd-pgp-genomics-files/VM_startup/assign-files.py /home/jeren
python3 /home/jeren/assign-files.py 

echo "RUNNING PIPELINE"

### Run Pipeline ###

### 1 - Preprocessing ###

echo "Running Preprocessing 1/2: python3 2-convert-23andMe-to-vcf.py "
python3 '/home/jeren/pipeline-scripts/1_Preprocessing/2-convert-23andMe-to-vcf.py'

echo "Running Preprocessing 2/2: python3 1-preprocess-CGI-v2.py "
python3 '/home/jeren/pipeline-scripts/1_Preprocessing/1-preprocess-CGI-v2.py'

### 2 - Imputation ###

echo "Running imputation: source imputation-run-all.sh "
source  /home/jeren/pipeline-scripts/2_Imputation/imputation-run-all.sh

### 3 - Post processing ###

echo "Running post processing: python3 post-process.py"
python3 /home/jeren/pipeline-scripts/3_Postprocessing/post-process.py

### Save serial port 1 logs ###

INSTANCE_NAME=$(curl -H "Metadata-Flavor: Google" "http://metadata/computeMetadata/v1/instance/name")

gcloud compute instances get-serial-port-output $INSTANCE_NAME --zone $ZONE --port 1 > /home/metadata/sp1_"$INSTANCE_NAME"_logs.txt
gsutil cp /home/metadata/sp1_${INSTANCE_NAME}_logs.txt gs://hvd-pgp-genomics-files/VM_run_logs/sp1_"$INSTANCE_NAME"_logs.txt

### Shutdown VM ###

# Delete the VM instance
gcloud compute instance-groups managed delete-instances $INSTANCE_GROUP  \
	--instances $INSTANCE_NAME \
	--zone $ZONE


