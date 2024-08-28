--    Copyright 2024 Google LLC

--   Licensed under the Apache License, Version 2.0 (the "License");
--   you may not use this file except in compliance with the License.
--   You may obtain a copy of the License at

--       http://www.apache.org/licenses/LICENSE-2.0

--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.

-- Based on the BigQuery Spark Stored Procedures shown in the following:
-- https://cloud.google.com/bigquery/docs/iceberg-tables
-- https://youtu.be/IQR9gJuLXbQ?feature=shared
-- https://github.com/GoogleCloudPlatform/data-analytics-golden-demo/blob/main/sql-scripts/rideshare_lakehouse_enriched/sp_iceberg_spark_transformation.py
-- https://cloud.google.com/bigquery/docs/spark-procedures

CREATE OR REPLACE PROCEDURE `${PROJECT_ID}`.${BIGQUERY_DATASET}.${BIGQUERY_STORED_PROCEDURE}()
WITH CONNECTION `${PROJECT_ID}.${BIGQUERY_LOCATION}.${BIGQUERY_SPARK_CONNECTION}`
OPTIONS(engine="SPARK",
jar_uris=["gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar"],
properties=[
("spark.jars.packages",                                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}",              "org.apache.iceberg.spark.SparkCatalog"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}.gcp_project",  "${PROJECT_ID}"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}.gcp_location", "${BIGQUERY_LOCATION}"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}.blms_catalog", "${ICEBERG_CATALOG_NAME}"),
("spark.sql.catalog.${ICEBERG_CATALOG_NAME}.warehouse",    "${GCS_BUCKET_LAKEHOUSE}")
]
)
LANGUAGE PYTHON AS R'''
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
import time
import sys
import os
import json

spark = SparkSession \
     .builder \
     .appName("BigLake Iceberg") \
     .config("spark.network.timeout", 50000) \
     .getOrCreate()

SOURCE_DATA_PARQUET = "${GCS_BUCKET_DATA_LOC}"
PROJECT_ID = "${PROJECT_ID}"
ICEBERG_CATALOG_NAME = "${ICEBERG_CATALOG_NAME}"
# Make BQ Dataset name == Iceberg Namespace for easier governance
ICEBERG_NAMESPACE_NAME = "${ICEBERG_NAMESPACE_NAME}"
BQ_DATASET_FOR_ICEBERG_TBL = "${BIGQUERY_DATASET}"
# Make BQ Table name == Iceberg Table for easier governance
ICEBERG_TABLE_NAME = "${ICEBERG_TABLE_NAME}" 
BQ_TABLE_FOR_ICEBERG_TBL = "${BIGQUERY_TABLENAME}"
BIG_LAKE_CONNECTION = "${PROJECT_ID}.${BIGQUERY_LOCATION}.${BIGQUERY_BIGLAKE_CONNECTION}"

# Useful notes: 
# In spark 3, Iceberg tables use identifies that have 3 parts 
# iceberg_catalog_name.iceberg_namespace_name.iceberg_table_name
# Specific to BigLake Metastore, tables have 3 different identifiers
# iceberg_catalog_name.iceberg_database_name.iceberg_table_name

# Create a catalog : https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_catalogs
spark.sql("CREATE NAMESPACE IF NOT EXISTS `{}`;".format(ICEBERG_CATALOG_NAME))

# Create a namespace in catalog aka BLMS "Database" : https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_databases 
spark.sql("CREATE NAMESPACE IF NOT EXISTS `{}`.{};".format(ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE_NAME))

# (optional) manual clean up
# spark.sql("DROP TABLE IF EXISTS `{}`.{}.{};".format(ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE_NAME, ICEBERG_TABLE_NAME))

# Create an Iceberg table in Spark and automatically create a BigLake Iceberg table at the same time
# N.B. The BigQuery dataset where you want to create tables must already exist. 
# This statement does not create a BigQuery dataset. 
# The location of BigQuery datasets and tables must be the same as the BigLake Metastore catalog.
# https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_tables
# https://cloud.google.com/bigquery/docs/manage-open-source-metadata#auto-link
spark.sql(
"CREATE TABLE IF NOT EXISTS `{}`.{}.{} ".format(ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE_NAME, ICEBERG_TABLE_NAME) + \
"(model STRING, mpg FLOAT, cyl INT, disp FLOAT, hp INT, drat FLOAT, wt FLOAT, qsec FLOAT, vs INT, am FLOAT, gear INT, carb INT) " + \
"USING iceberg " + \
"TBLPROPERTIES(bq_table='{}.{}', bq_connection='{}');".format(BQ_DATASET_FOR_ICEBERG_TBL, BQ_TABLE_FOR_ICEBERG_TBL, BIG_LAKE_CONNECTION)
)

# Load data from the RAW bucket into the newly created Iceberg Table
df_car_data = spark.read.parquet("{}".format(SOURCE_DATA_PARQUET))

df_car_data.createOrReplaceTempView("temp_view_car_data")

spark.sql(
"INSERT INTO `{}`.{}.{} ".format(ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE_NAME, ICEBERG_TABLE_NAME) + \
"(model, mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb) " + \
"SELECT * FROM temp_view_car_data;"
)
'''