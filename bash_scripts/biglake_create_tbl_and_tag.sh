#!/bin/bash

#    Copyright 2024 Google LLC

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# --------------------------------------------------------------------------------------
# Based on : 
# https://cloud.google.com/data-catalog/docs/tag-bigquery-dataset#gcloud
# 
# Run these evironmental variables first
# https://github.com/ant-laz/biglake-iceberg-python?tab=readme-ov-file#approach-b--dataproc-serverless-batch-job--biglake-metastore--spark--iceberg
#
# N.B. Currently, Data Catalog supports only the us-central1 region.
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# Create a Data Catalog Tag Template.
# --------------------------------------------------------------------------------------
TAG_TEMPLATE_NAME="iceberg_template"

# N.B. Currently, Data Catalog supports only the us-central1 region.
DATA_CATALOG_LOCATION="us-central1"

gcloud data-catalog tag-templates create ${TAG_TEMPLATE_NAME} \
    --location=${DATA_CATALOG_LOCATION} \
    --display-name="BigLake Iceberg Tag Template" \
    --field=id=has_pii,display-name="Has PII",type=bool

# --------------------------------------------------------------------------------------
# Create a BigQuery BigLake Iceberg Table with BigLake Metastore
# https://github.com/ant-laz/biglake-iceberg-python?tab=readme-ov-file#approach-b--dataproc-serverless-batch-job--biglake-metastore--spark--iceberg
# --------------------------------------------------------------------------------------

CONFS="spark.sql.catalog.${ICEBERG_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,"
CONFS+="spark.sql.catalog.${ICEBERG_CATALOG_NAME}.catalog-impl=org.apache.iceberg.gcp.biglake.BigLakeCatalog,"
CONFS+="spark.sql.catalog.${ICEBERG_CATALOG_NAME}.gcp_project=${PROJECT_ID},"
CONFS+="spark.sql.catalog.${ICEBERG_CATALOG_NAME}.gcp_location=${BIGQUERY_LOCATION},"
CONFS+="spark.sql.catalog.${ICEBERG_CATALOG_NAME}.blms_catalog=${ICEBERG_CATALOG_NAME},"
CONFS+="spark.sql.catalog.${ICEBERG_CATALOG_NAME}.warehouse=${GCS_BUCKET_LAKEHOUSE},"
CONFS+="spark.jars.packages=${ICEBERG_SPARK_PACKAGE}"

gcloud dataproc batches submit pyspark ../pyspark_jobs/iceberg_table_creation.py \
  --properties="${CONFS}" \
  --jars="${BIGLAKE_ICEBERG_CATALOG_JAR}" \
  --region="${DATA_PROC_REGION}" \
  --service-account="${SERVICE_ACCT_FULL}" \
  --deps-bucket="${GCS_BUCKET_DATAPROC_DEPS}" \
  --version=2.2 \
  -- --project_id=${PROJECT_ID} --iceberg_catalog_name=${ICEBERG_CATALOG_NAME} --iceberg_namespace_name=${ICEBERG_NAMESPACE_NAME} --iceberg_table_name=${ICEBERG_TABLE_NAME} --bigquery_dataset=${BIGQUERY_DATASET} --bigquery_table=${BIGQUERY_TABLENAME} --big_lake_connection="${PROJECT_ID}.${BIGQUERY_LOCATION}.${BIGQUERY_BIGLAKE_CONNECTION}" --source_data_parquet=${GCS_BUCKET_DATA_LOC}

# -------------------------------
# Lookup the Data Catalog entry for the table.
# -------------------------------
ENTRY_NAME=$(gcloud data-catalog entries lookup "//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/${BIGQUERY_DATASET}/tables/${BIGQUERY_TABLENAME}" --format="value(name)")

# -------------------------------
# Attach a Tag to the table.
# -------------------------------
cat > tag_file.json << EOF
  {
    "has_pii": false,
  }
EOF

gcloud data-catalog tags create \
  --entry=${ENTRY_NAME} \
  --location=$DATA_CATALOG_LOCATION \
  --tag-template="projects/${PROJECT_ID}/locations/us-central1/tagTemplates/${TAG_TEMPLATE_NAME}" \
  --tag-file=tag_file.json
