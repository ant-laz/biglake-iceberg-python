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

# Based on code from these sources:
# https://github.com/GoogleCloudPlatform/dataproc-templates/blob/main/python/main.py


import logging
import argparse
from typing import Dict
from pyspark.sql import SparkSession
from logging import Logger


def get_logger(spark: SparkSession) -> Logger:
     """
     based on : https://github.com/GoogleCloudPlatform/dataproc-templates/blob/main/python/dataproc_templates/base_template.py#L29-L41
     """
     log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
     return log_4j_logger.LogManager.getLogger(__name__)

def create_iceberg_tables(spark: SparkSession, args: dict):

    # grab the logger from the spark context
    logger: Logger = get_logger(spark=spark)

    # log the input we received

    # execute the creation of spark table...
    logger.info('Creating iceberg method called')
    logger.info('args supplied: {}'.format(args))

    # namespace is a general programming concept with gives a context for identifiers [1] . 
    # when talking about Spark, Iceberg, BLMS & BigQuery we need identifiers for tables
    # In this environment, namespaces are used to organise tables.
    # Spark, BigLake Metastore & BigQuery all use three-level namespaces.
    # ==> Spark:     catalog.database.table
    # ==> BLMS:      catalog.database.table
    # ==> BigQuery:  project.dataset.table
    #
    # In Spark SQL, you can create namespaces & nest them to create multi-level namespaces
    # CREATE NAMESPACE {}    ==> creates a catalog
    # CREATE NAMESPACE {}.{} ==> nesting, creates 2-level namespace ....catalog.db
    # CREATE TABLE {}.{}.{}  ==> nesting, creates 3-level namespace ....catalog.db.table

    # Create a catalog : https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_catalogs
    spark.sql("CREATE NAMESPACE IF NOT EXISTS `{}`;".format(args["iceberg_catalog_name"]))

    # Create a database : https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_databases 
    spark.sql("CREATE NAMESPACE IF NOT EXISTS `{}`.{};".format(args["iceberg_catalog_name"], args["iceberg_namespace_name"]))

    # Create an Iceberg table in Spark and automatically create a BigLake Iceberg table at the same time
    # N.B. The BigQuery dataset where you want to create tables must already exist. 
    # This statement does not create a BigQuery dataset. 
    # The location of BigQuery datasets and tables must be the same as the BigLake Metastore catalog.
    # https://cloud.google.com/bigquery/docs/manage-open-source-metadata#create_tables
    # https://cloud.google.com/bigquery/docs/manage-open-source-metadata#auto-link
    spark.sql(
    "CREATE TABLE IF NOT EXISTS `{}`.{}.{} ".format(args["iceberg_catalog_name"], args["iceberg_namespace_name"], args["iceberg_table_name"]) + \
    "(model STRING, mpg FLOAT, cyl INT, disp FLOAT, hp INT, drat FLOAT, wt FLOAT, qsec FLOAT, vs INT, am FLOAT, gear INT, carb INT) " + \
    "USING iceberg " + \
    "TBLPROPERTIES(bq_table='{}.{}', bq_connection='{}');".format(args["bigquery_dataset"], args["bigquery_table"], args["big_lake_connection"])
    )

    # Load data from the RAW bucket into the newly created Iceberg Table
    df_car_data = spark.read.parquet("{}".format(args["source_data_parquet"]))

    df_car_data.createOrReplaceTempView("temp_view_car_data")

    spark.sql(
    "INSERT INTO `{}`.{}.{} ".format(args["iceberg_catalog_name"], args["iceberg_namespace_name"], args["iceberg_table_name"]) + \
    "(model, mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb) " + \
    "SELECT * FROM temp_view_car_data;"
    )    
    

def create_spark_session() -> SparkSession:

    #create a spark session
    spark: SparkSession = SparkSession.builder \
    .appName("BigLake Iceberg") \
    .config("spark.network.timeout", 50000) \
    .getOrCreate() 

    return spark

def parse_cmd_arguments() -> Dict:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--project_id',
            dest="project_id",
            required=True,
            help='GCP Project ID'
        )

        parser.add_argument(
            f'--iceberg_catalog_name',
            dest="iceberg_catalog_name",
            required=True,
            help='Iceberg catalog Name'
        )

        parser.add_argument(
            f'--iceberg_namespace_name',
            dest="iceberg_namespace_name",
            required=True,
            help='Iceberg namespace name'
        )

        parser.add_argument(
            f'--iceberg_table_name',
            dest="iceberg_table_name",
            required=True,
            help='Iceberg table name'
        )

        parser.add_argument(
            f'--bigquery_dataset',
            dest="bigquery_dataset",
            required=True,
            help='BigQuery Dataset'
        )

        parser.add_argument(
            f'--bigquery_table',
            dest="bigquery_table",
            required=True,
            help='BigQuery Table'
        )

        parser.add_argument(
            f'--big_lake_connection',
            dest="big_lake_connection",
            required=True,
            help='BigLake connection'
        )

        parser.add_argument(
            f'--source_data_parquet',
            dest="source_data_parquet",
            required=True,
            help='GCS location of source Parquet data for Iceberg tables'
        )        

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args() 

        return vars(known_args)

def launch_pipeline():
    args: dict = parse_cmd_arguments()

    spark: SparkSession = create_spark_session()

    create_iceberg_tables(spark, args)

if __name__ == '__main__':
    launch_pipeline()