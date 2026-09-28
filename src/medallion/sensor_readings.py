"""Silver sensor_readings: one validated row per station-minute from the live sensors (docs/PLAN.md step 9).

Bronze simulator_readings already has one row per message with typed values, so there's nothing to flatten: parse the
sensor's time string, validate, and hand over to the shared silver mechanics (silver.py). The physical ranges are the
same as the hourly readings'. Duplicates (SQS redeliveries, sensor resends) collapse on the key.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from medallion.readings import MEASURES, RANGES
from medallion.silver import collect_reasons

KEY = ("station_id", "observed_at")
LINEAGE = ("event_id", "_batch_id", "_message_id", "_ingested_at")
TIEBREAK = ("_batch_id", "_message_id")  # after _ingested_at: dedup_latest's order for equal keys
SENSOR_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"  # 2026-09-28T17:16:00Z, as the sensors send it

SILVER_COLUMNS = (*KEY, *MEASURES, *LINEAGE, "_merged_at")
QUARANTINE_COLUMNS = ("station_id", "observed_at_raw", "observed_at", *MEASURES, "rejection_reasons", *LINEAGE, "_quarantined_at")
# Each bronze row is one SQS delivery: a rejected delivery is recorded once, however often the job sees it.
QUARANTINE_MATCH = ("_message_id",)


def add_observed_at(bronze: DataFrame) -> DataFrame:
    """Keeps the sensor's string as observed_at_raw and parses it to a UTC timestamp; unparseable -> null, then rejected."""
    return bronze.withColumnRenamed("observed_at", "observed_at_raw").withColumn(
        "observed_at", F.try_to_timestamp(F.col("observed_at_raw"), F.lit(SENSOR_TIME_FORMAT))
    )


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Same null handling as readings: a missing value is its own rule, and ranges are only checked on present values."""
    rules = [
        (F.col("station_id").isNull(), "missing_station_id"),
        (F.col("observed_at").isNull(), "invalid_observed_at"),
    ]
    for column in MEASURES:
        low, high = RANGES[column]
        rules.append((F.col(column).isNull(), f"missing_{column}"))
        rules.append((F.col(column).isNotNull() & ~F.col(column).between(low, high), f"out_of_range_{column}"))
    return collect_reasons(df, rules)


def to_silver(unique: DataFrame) -> DataFrame:
    return unique.withColumn("_merged_at", F.current_timestamp()).select(*SILVER_COLUMNS)


def to_quarantine(rejected: DataFrame) -> DataFrame:
    return rejected.dropDuplicates(list(QUARANTINE_MATCH)).withColumn("_quarantined_at", F.current_timestamp()).select(*QUARANTINE_COLUMNS)
