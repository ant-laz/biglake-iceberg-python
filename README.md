# biglake-iceberg-python

## What problem does this repo solve ? 

[Documentation](https://cloud.google.com/bigquery/docs/iceberg-tables) and [videos](https://www.youtube.com/watch?v=IQR9gJuLXbQ) for creating BigLake Iceberg tables is focused on 
using BigQuery PySpark Stored Proceures. 

For some teams this approach is not convenient. 

Documentation does not exist for other approaches, this creates fricton for teams
looking to get started with BigLake Iceberg tables. 

## What solution does this repo offer ? 

This repo offers detail instructions on different approaches to creating BigLake
Iceberg tables using Python & Spark.

This aims to reduce friction for teams looking to get started with BigLake Iceberg 
tables by hopefully included at least 1 approach aligned to their current architecture
and tech stack.

## Approach A : BQ Stored Procedures + BigLake Metastore + Spark + Iceberg

### Approach A  - Overview

![approach_a_architecture](images/a_architecture.png)

1. A GCS bucket where we land our raw data, e.g. Parquet files 
2. A Cloud Resource BigQuery connection that delegates creating BigLake tables to #3
3. A Spark BigQuery connection that is the main driver of Lakehouse creation
4. A BigQuery Spark Stored Procedure that has the PySpark code to create our Lakehouse
5. A Dataproc serverless cluster that executes the store procedure #4
6. A GCS bucket which has the files of our Lakehouse (metadata + data files)
7. A BigLake Metastore which acts as our Iceberg catalog, mapping table IDs to metadat pointers
8. BigLake tables, automatically created at the same time as (& kept in sync with) Iceberg tables

### Approach A  - step 1 of 4 - Initial setup

Based on the instructions [here](https://youtu.be/IQR9gJuLXbQ?feature=shared) & 
also based on these [instructions](https://cloud.google.com/bigquery/docs/iceberg-tables)

create some environmental variables
```
export PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")
export IAM_CUSTOM_ROLE_ID="CustomDelegate"
export BIGQUERY_LOCATION="US"
export GCS_LOCATION="US"
export BIGQUERY_STORED_PROCEDURE="my_biglake_stored_procedure"
export BIGQUERY_SPARK_CONNECTION="bigspark-connection"
export BIGQUERY_BIGLAKE_CONNECTION="biglake-connection"
export GCS_BUCKET_LAKEHOUSE="gs://${PROJECT_ID}-iceberg-datalakehouse"
export GCS_BUCKET_RAW_DATA="gs://${PROJECT_ID}-raw-data-parquet"
export GCS_BUCKET_RAW_DATA_TBL="${GCS_BUCKET_DATA}/my_cars_tbl/"
export LOCAL_DATA_LOCATION="data/mt_cars.parquet"
export GCS_BUCKET_DATA_LOC="${GCS_BUCKET_DATA_TBL}*.parquet"
export ICEBERG_CATALOG_NAME="my_datalakehouse_iceberg_catalog"
export ICEBERG_NAMESPACE_NAME="iceberg_datasets"
export BIGQUERY_DATASET="iceberg_datasets"
export ICEBERG_TABLE_NAME="cars"
export BIGQUERY_TABLENAME="cars"
```

enable apis
```
gcloud services enable iam.googleapis.com bigqueryconnection.googleapis.com biglake.googleapis.com
```

### Approach A  - step 2 of 4 - RAW Data / Landing Zone

create a GCS bucket

```
gcloud storage buckets create ${GCS_BUCKET_RAW_DATA} \
  --project=${PROJECT_ID} \
  --location=${GCS_LOCATION} \
  --uniform-bucket-level-access  
```

On your local machine, upload Parquet file to the data bucket
```
gcloud storage cp ${LOCAL_DATA_LOCATION} ${GCS_BUCKET_RAW_DATA_TBL}
```

### Approach A  - step 3 of 4 - Creating the Lakehouse

create a custom IAM role
```
gcloud iam roles create ${IAM_CUSTOM_ROLE_ID} \
  --project=${PROJECT_ID} \
  --title="${IAM_CUSTOM_ROLE_ID}" \
  --description="${IAM_CUSTOM_ROLE_ID}" \
  --permissions="bigquery.connections.delegate" \
  --stage=GA
```

create a bigquery dataset
```
bq --location=${BIGQUERY_LOCATION} mk \
  --dataset \
  ${PROJECT_ID}:${BIGQUERY_DATASET}
```
create a GCS bucket for our data lakehouse

```
gcloud storage buckets create ${GCS_BUCKET_LAKEHOUSE} \
  --project=${PROJECT_ID} \
  --location=${GCS_LOCATION} \
  --uniform-bucket-level-access
```

Create a BigQuery Apahce Spark [Connection](https://cloud.google.com/bigquery/docs/connect-to-spark#bq)
```
bq mk --connection \
  --connection_type='SPARK' \
  --project_id=${PROJECT_ID} \
  --location=${BIGQUERY_LOCATION} \
  ${BIGQUERY_SPARK_CONNECTION}
```

Grab the service account created for this connection
```
export SPARK_SA=$(bq show --format="json" --connection ${PROJECT_ID}.${BIGQUERY_LOCATION}.${BIGQUERY_SPARK_CONNECTION} | jq -r .spark.serviceAccountId)
```

Create a BigQuery Cloud Resource [Connection](https://cloud.google.com/bigquery/docs/create-cloud-resource-connection)
```
bq mk --connection \
  --connection_type='CLOUD_RESOURCE' \
  --project_id=${PROJECT_ID} \
  --location=${BIGQUERY_LOCATION} \
  ${BIGQUERY_BIGLAKE_CONNECTION}
```

Grab the service account created for this connection
```
export BIGLAKE_SA=$(bq show --format="json" --connection ${PROJECT_ID}.${BIGQUERY_LOCATION}.${BIGQUERY_BIGLAKE_CONNECTION} | jq -r .cloudResource.serviceAccountId)
```

Enable our Apache Spark connection to carry operations on GCS & BigQuery
```
gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET_LAKEHOUSE} \
  --member="serviceAccount:${SPARK_SA}" \
  --role=roles/storage.objectAdmin

gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET_RAW_DATA} \
  --member="serviceAccount:${SPARK_SA}" \
  --role=roles/storage.objectViewer

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SPARK_SA}" \
  --role=roles/biglake.admin

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SPARK_SA}" \
  --role=roles/bigquery.user
```

Use the BigQuery UI to allow the Spark Connection service account `SPARK_SA` to have
the custom role `CustomDelegate` on the Resource Connection `BIGQUERY_BIGLAKE_CONNECTION`.
This unfortunatley cannot be done via command line tools.

Use the BigQuery UI to allow the Spark Connection service account `SPARK_SA` to have
the role `BigQuery Data Owner` on the Dataset `BIGQUERY_DATASET`.
This unfortunatley cannot be done via command line tools easily.

Enable our Cloud Resource connection to carry out operations.
```
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${BIGLAKE_SA}" \
  --role=roles/biglake.admin

gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET_LAKEHOUSE} \
  --member="serviceAccount:${BIGLAKE_SA}" \
  --role=roles/storage.objectAdmin
```

Use the BigQuery UI to allow the Spark Connection service account `BIGLAKE_SA` to have
the role `BigQuery Data Owner` on the Dataset `BIGQUERY_DATASET`.
This unfortunatley cannot be done via command line tools easily.

**With the above setup complete, we use a BigQuery Spark Stored Procedure to create the Iceberg catalog + tables**

**An example stored procedure can be found [iceberg_stored_procedure.sql](/bigquery_stored_procedures/iceberg_stored_procedure.sql)**



Explaining the spark properties   

| Spark Property  | Value | Explanation |
| ------------- | ------------- | ------------- |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`  | org.apache.iceberg.spark.SparkCatalog  | An Apche Iceberg Catalog with name `${ICEBERG_CATALOG_NAME}`  should be created & managed using implementation class org.apache.iceberg.spark.SparkCatalog  |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`.catalog-impl  | org.apache.iceberg.gcp.biglake.BigLakeCatalog  | We are using a Custom Iceberg catalog implementation as defined by class org.apache.iceberg.gcp.biglake.BigLakeCatalog  |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`.gcp_project  | `${PROJECT_ID}`  | Required by the custom iceberg catalog implementation org.apache.iceberg.gcp.biglake.BigLakeCatalog. The GCP project that contains the catalog  |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`.gcp_location  | `${BIGQUERY_LOCATION}`  |Required by the custom iceberg catalog implementation org.apache.iceberg.gcp.biglake.BigLakeCatalog. Within GCP project, the GCP location that contains the catalog  |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`.blms_catalog  | `${ICEBERG_CATALOG_NAME}`  | Required by the custom iceberg catalog implementation org.apache.iceberg.gcp.biglake.BigLakeCatalog. The name of the BigLake Metastore catalog corresponding to this Iceberg Catalog.  |
| spark.sql.catalog.`${ICEBERG_CATALOG_NAME}`.warehouse  | `${GCS_BUCKET_LAKEHOUSE}`  | Warehouse location for the catalog to store data. This is a GCS bucket on Google Cloud. This single bucket will contain both the Iceberg metadata layer (metadata files, manifest list & manifest file) and the Iceberg data layer (Parquet files).   |
| spark.jars.packages  | org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0  | Includes the  Iceberg classes that Spark needs to interact with Iceberg tables and metadata.  |

### Approach A  - step 4 of 4 - Inspecting the Data lakehouse created

List out the catalogs created in the BigQuery BigLake Metastore

```
curl "https://biglake.googleapis.com/v1/projects/${PROJECT_ID}/locations/${BIGQUERY_LOCATION}/catalogs" \
  --header "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  --header "Accept: application/json"
```

List out databases in Catalog `${ICEBERG_CATALOG_NAME}`

```
curl "https://biglake.googleapis.com/v1/projects/${PROJECT_ID}/locations/${BIGQUERY_LOCATION}/catalogs/${ICEBERG_CATALOG_NAME=}/databases" \
  --header "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  --header "Accept: application/json"
```

List out tables in Catalog `${ICEBERG_CATALOG_NAME}` & database `${ICEBERG_NAMESPACE_NAME}`

```
curl "https://biglake.googleapis.com/v1/projects/${PROJECT_ID}/locations/${BIGQUERY_LOCATION}/catalogs/${ICEBERG_CATALOG_NAME=}/databases/${ICEBERG_NAMESPACE_NAME}/tables" \
  --header "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  --header "Accept: application/json"
```

## Approach B : Dataproc Serverless Batch Job + BigLake Metastore + Spark + Iceberg

todo

## Approach C : Dataproc CE + BigLake Metastore + Spark + Iceberg

todo

## Approach D : Dataproc VM + BigLake Metastore + Spark + Iceberg

todo

## Approach E : Dataproc Serverless Notebook + BigLake Metastore + Spark + Iceberg

todo


