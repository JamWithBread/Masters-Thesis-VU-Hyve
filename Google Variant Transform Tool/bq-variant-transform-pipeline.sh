#!/bin/bash

# bq-variant-transform-pipeline.sh

# config parameters:
GOOGLE_CLOUD_PROJECT=hyve-ohdsi-genomics-masters
GOOGLE_CLOUD_REGION=europe-west4
GOOGLE_CLOUD_LOCATION=europe-west4
TEMP_LOCATION=gs://hvd-pgp-genomics-files/temp
INPUT_PATTERN=gs://hvd-pgp-genomics-files/final_output/*.vcf
OUTPUT_TABLE=hyve-ohdsi-genomics-masters:hvd_pgp_participant_variants.hvd_pgp_variants

### - Num workers = 5 , 4 cores each - ###
  COMMAND="vcf_to_bq \
  --input_pattern ${INPUT_PATTERN} \
  --output_table ${OUTPUT_TABLE} \
  --job_name vcf-to-bigquery \
  --runner DataflowRunner \
  --num_workers 5 \
  --max_num_workers 5 \
  --worker_machine_type n1-standard-4 \
  --disk_size_gb 100 \
  --worker_disk_type compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
  --keep_intermediate_avro_files"


docker run -v ~/.config:/root/.config \
  gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --location "${GOOGLE_CLOUD_LOCATION}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --temp_location "${TEMP_LOCATION}" \
  "${COMMAND}"


