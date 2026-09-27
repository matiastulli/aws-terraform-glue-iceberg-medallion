import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from medallion.bronze import add_ingestion_metadata, source_schema


def test_raw_files_are_read_with_the_table_schema_minus_added_columns():
    table = StructType([
        StructField("latitude", DoubleType()),
        StructField("station_id", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
    ])
    assert source_schema(table).fieldNames() == ["latitude"]


def test_station_id_comes_from_the_file_name(spark):
    df = spark.createDataFrame([("s3://raw/open_meteo/hourly/date=2026-09-20/buenos_aires.json",)], "path string")

    row = add_ingestion_metadata(df, "run-1", F.col("path")).first()

    assert (row.station_id, row._batch_id, row._source_file) == ("buenos_aires", "run-1", row.path)


def test_a_file_not_named_after_a_station_fails_the_load(spark):
    # Without a station_id a reading has no key, so the load must fail rather than write it.
    df = spark.createDataFrame([("s3://raw/open_meteo/hourly/date=2026-09-20/Buenos Aires.json",)], "path string")

    with pytest.raises(Exception, match="not named <station_id>.json"):
        add_ingestion_metadata(df, "run-1", F.col("path")).collect()
