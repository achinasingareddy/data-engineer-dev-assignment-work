"""
---------------------------------------------------------
File: bronze_to_silver.py
Description:
Read Bronze JSON and create Silver + Quarantine layers.
---------------------------------------------------------
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (array,col,current_timestamp,explode,lit,lower,trim,when)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

COMMON_PATH ="/Volumes/dev_engineering_catalog/dev_engineering_schema/dev_engineering_volume"
BRONZE_PATH = f"{COMMON_PATH}/bronze/university_chapters"
SILVER_PATH = f"{COMMON_PATH}/silver/university_chapters/"
QUARANTINE_PATH = f"{COMMON_PATH}/quarantine/university_chapters/"

STATUS_OK = "OK"
STATUS_WARNING = "WARNING"
INVALID_COORDINATES = "INVALID_COORDINATES"

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("BronzeToSilver")

# ---------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .getOrCreate()
)

# ---------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------


def latest_run_folder(bronze_path: str) -> tuple[str, str]:
    """
    Returns latest run folder and raw json path.
    """

    folders = sorted(
        [
            p.name
            for p in Path(bronze_path).iterdir()
            if p.is_dir()
        ]
    )

    if not folders:
        raise FileNotFoundError("No Bronze folders found.")

    latest = folders[-1]

    return latest, str(Path(bronze_path) / latest / "raw.json")


def read_bronze(path: str):

    logger.info("Reading Bronze JSON : %s", path)

    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )


def flatten(df):

    logger.info("Flattening JSON")

    return (
        df
        .select(explode("features").alias("feature"))
        .select(
            col("feature.attributes.ChapterID").alias("chapter_id"),
            col("feature.attributes.University_Chapter").alias("chapter_name"),
            col("feature.attributes.City").alias("city"),
            col("feature.attributes.State").alias("state"),
            col("feature.geometry.x").cast("double").alias("longitude"),
            col("feature.geometry.y").cast("double").alias("latitude")
        )
    )


def invalid_coordinate_condition():

    return (
        col("longitude").isNull()
        | col("latitude").isNull()
        | (col("longitude") < -180)
        | (col("longitude") > 180)
        | (col("latitude") < -90)
        | (col("latitude") > 90)
    )


def warning_condition():

    return (
        col("city").isNull()
        | (trim(col("city")) == "")
        | (lower(trim(col("city"))) == "unknown")
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

try:

    run_id, bronze_json = latest_run_folder(BRONZE_PATH)
    logger.info("Processing Run : %s", run_id)
    bronze_df = read_bronze(bronze_json)
    silver_df = flatten(bronze_df)
    rows_in = silver_df.count()
    invalid_condition = invalid_coordinate_condition()
    quarantine_df = (
        silver_df
        .filter(invalid_condition)
        .withColumn("reason_code", lit(INVALID_COORDINATES))
        .withColumn("ingest_run_id", lit(run_id))
        .withColumn("ingest_timestamp", current_timestamp())
    )

    valid_df = silver_df.filter(~invalid_condition)

    warn_condition = warning_condition()

    silver_final = (
        valid_df
        .withColumn("dq_status",when(warn_condition,lit(STATUS_WARNING)).otherwise(lit(STATUS_OK)))
        .withColumn("dq_warnings",when(warn_condition,array(lit("MISSING_OR_UNKNOWN_CITY")))
        .otherwise(array()))
        .withColumn("ingest_run_id",lit(run_id))
        .withColumn("ingest_timestamp",current_timestamp())
    )

    # -----------------------------------------------------
    # Write Silver
    # -----------------------------------------------------

    (
        silver_final.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    # -----------------------------------------------------
    # Write Quarantine
    # -----------------------------------------------------

    (
        quarantine_df.write
        .mode("overwrite")
        .parquet(f"{QUARANTINE_PATH}/{run_id}")
    )

    # -----------------------------------------------------
    # Metrics
    # -----------------------------------------------------

    status = {
        r["dq_status"]: r["count"]
        for r in silver_final.groupBy("dq_status").count().collect()
    }

    rows_ok = status.get(STATUS_OK, 0)
    rows_warn = status.get(STATUS_WARNING, 0)
    rows_quarantine = quarantine_df.count()

    logger.info("=================================")
    logger.info("Bronze -> Silver Completed")
    logger.info("=================================")
    logger.info("Rows In          : %s", rows_in)
    logger.info("Rows OK          : %s", rows_ok)
    logger.info("Rows Warning     : %s", rows_warn)
    logger.info("Rows Quarantine  : %s", rows_quarantine)
    logger.info("=================================")

except Exception as ex:

    logger.exception("Pipeline Failed : %s", ex)
    raise