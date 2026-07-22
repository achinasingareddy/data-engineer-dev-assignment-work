"""
---------------------------------------------------------
File: unit_testing.py
Description:
Unit Tests for Bronze -> Silver Data Quality Rules
---------------------------------------------------------
"""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    lit,
    lower,
    trim,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

STATUS_OK = "OK"
STATUS_WARNING = "WARNING"

INVALID_COORDINATES = "INVALID_COORDINATES"
UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"



# ---------------------------------------------------------------------
# DQ Rules
# ---------------------------------------------------------------------

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
# Helper Methods
# ---------------------------------------------------------------------

def split_valid_invalid(df):

    invalid = invalid_coordinate_condition()

    quarantine = (

        df.filter(invalid)

        .withColumn(
            "reason_code",
            lit(INVALID_COORDINATES)
        )

    )

    valid = df.filter(~invalid)

    return valid, quarantine


def apply_warning(df):

    warning = warning_condition()

    return (

        df

        .withColumn(

            "dq_status",

            when(
                warning,
                lit(STATUS_WARNING)
            ).otherwise(
                lit(STATUS_OK)
            )

        )

        .withColumn(

            "dq_warnings",

            when(
                warning,
                array(lit(UNKNOWN_CITY))
            ).otherwise(
                array()
            )

        )

    )

# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------

def test_invalid_records_go_to_quarantine(sample_dataframe):

    valid, quarantine = split_valid_invalid(sample_dataframe)

    assert quarantine.count() == 1

    assert valid.count() == 2

    assert quarantine.first()["reason_code"] == INVALID_COORDINATES


@pytest.mark.parametrize(

    "city,status",

    [

        ("UNKNOWN", STATUS_WARNING),

        ("unknown", STATUS_WARNING),

        ("", STATUS_WARNING),

        (None, STATUS_WARNING),

        ("Los Angeles", STATUS_OK)

    ]

)

def test_city_warning_rule(spark, city, status):

    df = spark.createDataFrame(

        [

            (

                "ID1",

                "University",

                city,

                "CA",

                -120.0,

                35.0

            )

        ],

        [

            "chapter_id",

            "chapter_name",

            "city",

            "state",

            "longitude",

            "latitude"

        ]

    )

    result = apply_warning(df)

    assert result.first()["dq_status"] == status


def test_gold_has_no_invalid_coordinates(sample_dataframe):

    valid, _ = split_valid_invalid(sample_dataframe)

    gold = apply_warning(valid)

    assert (

        gold.filter(

            (col("longitude") > 180)

            | (col("latitude") > 90)

        ).count()

        == 0

    )


def test_required_columns(sample_dataframe):

    valid, _ = split_valid_invalid(sample_dataframe)

    silver = apply_warning(valid)

    expected = {

        "chapter_id",

        "chapter_name",

        "city",

        "state",

        "longitude",

        "latitude",

        "dq_status",

        "dq_warnings"

    }

    assert expected.issubset(set(silver.columns))


def test_record_counts(sample_dataframe):

    valid, quarantine = split_valid_invalid(sample_dataframe)

    silver = apply_warning(valid)

    assert sample_dataframe.count() == 3

    assert silver.count() == 2

    assert quarantine.count() == 1


def test_no_duplicate_chapter_ids(sample_dataframe):

    duplicates = (

        sample_dataframe

        .groupBy("chapter_id")

        .count()

        .filter(col("count") > 1)

    )

    assert duplicates.count() == 0


def test_latitude_range(sample_dataframe):

    valid, _ = split_valid_invalid(sample_dataframe)

    invalid = valid.filter(

        (col("latitude") < -90)

        | (col("latitude") > 90)

    )

    assert invalid.count() == 0


def test_longitude_range(sample_dataframe):

    valid, _ = split_valid_invalid(sample_dataframe)

    invalid = valid.filter(

        (col("longitude") < -180)

        | (col("longitude") > 180)

    )

    assert invalid.count() == 0

def main():
    # Create spark session directly (not using pytest fixtures)
    # Note: On Databricks, do not set .master() - it's auto-configured
    spark_session = SparkSession.builder.appName("DQUnitTests").getOrCreate()
    
# ---------------------------------------------------------------------
# Sample Data
# ---------------------------------------------------------------------

    schema = StructType([
        StructField("chapter_id", StringType()),
        StructField("chapter_name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType())
    ])
    
    data = [
        ("CA-001", "University A", "Los Angeles", "CA", -118.24, 34.05),
        ("CA-002", "University B", "UNKNOWN", "CA", -112.90, 37.33),
        ("CA-003", "University C", "Seattle", "WA", 250.00, 100.00)
    ]
    
    df = spark_session.createDataFrame(data, schema)
    
    # Run the test
   
    test_no_duplicate_chapter_ids(df)
    invalid_coordinate_condition()
    test_record_counts(df)
    

    
if __name__ == "__main__":
    main()