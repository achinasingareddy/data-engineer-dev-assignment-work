"""
---------------------------------------------------------
File: silver_to_gold.py
Description:
Read Silver Delta table and publish Gold Data Product
---------------------------------------------------------
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,current_timestamp,count,when,lit)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

COMMON_PATH = "/Volumes/dev_engineering_catalog/dev_engineering_schema/dev_engineering_volume"
SILVER_PATH = f"{COMMON_PATH}/silver/university_chapters"
GOLD_PATH = f"{COMMON_PATH}/gold/university_chapters/v1"
STATUS_OK = "OK"
STATUS_WARNING = "WARNING"

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("SilverToGold")

# -------------------------------------------------------------------
# Spark
# -------------------------------------------------------------------

def create_spark():

    return (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )

# -------------------------------------------------------------------
# Read Silver
# -------------------------------------------------------------------

def read_silver(spark):

    logger.info("Reading Silver Layer...")

    return (
      spark.read
        .parquet(SILVER_PATH)
    )

# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate(df):

    row_count = df.count()
    if row_count == 0:
        raise ValueError("Silver layer contains no records.")
    logger.info("Rows available : %s", row_count)
    return row_count

# -------------------------------------------------------------------
# Gold Transformation
# -------------------------------------------------------------------

def build_gold(df):

    logger.info("Building Gold dataset...")
    return (

        df.select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "dq_status",
            "dq_warnings"
        )
        .withColumn("published_timestamp",current_timestamp())
        .withColumn( "data_product",lit("University Chapters"))
        )

# -------------------------------------------------------------------
# Publish
# -------------------------------------------------------------------

def write_gold(df):
    logger.info("Writing Gold Layer...")
    (
        df.write
        .mode("overwrite")
        .parquet(GOLD_PATH)

    )
    df.display()
    logger.info("Gold Layer Published.")

# -------------------------------------------------------------------
# Metrics
# -------------------------------------------------------------------

def collect_metrics(df):

    logger.info("Collecting Metrics...")
    metrics = (
        df.agg(
            count("*").alias("total"),
            count(when(col("dq_status") == STATUS_OK,True)).alias("ok"),
            count(when(col("dq_status") == STATUS_WARNING,True)
            ).alias("warning")
        )
        .collect()[0]
    )

    logger.info("====================================")
    logger.info("Gold Publish Summary")
    logger.info("====================================")
    logger.info("Total Records : %s", metrics["total"])
    logger.info("OK Records    : %s", metrics["ok"])
    logger.info("Warnings      : %s", metrics["warning"])
    logger.info("====================================")

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():

    spark = create_spark()
    try:
        silver_df = read_silver(spark)
        validate(silver_df)
        gold_df = build_gold(silver_df)
        write_gold(gold_df)
        collect_metrics(gold_df)
        logger.info("Silver -> Gold completed successfully.")

    except Exception as ex:

        logger.exception("Pipeline Failed : %s", ex)

        raise

# -------------------------------------------------------------------

if __name__ == "__main__":

    main()