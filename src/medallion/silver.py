"""Silver: one typed, validated row per station-hour, from bronze's one-row-per-station-day API responses.

Pipeline, all pure DataFrame functions (the MERGEs stay in the Glue job):

    flatten_hourly  -> one row per hour, source columns renamed to the column convention
    add_observed_at -> the source's local time string -> a UTC timestamp
    add_rejection_reasons -> every rule that fails, as a list (empty = valid)
    dedup_latest    -> one row per (station_id, observed_at): the most recently ingested wins
    reconcile       -> every input row is accounted for exactly once, or the job fails before writing
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

KEY = ("station_id", "observed_at")

# Source variable -> silver column: the unit moves into the name, so each unit is checked against the source's units.
RENAMES = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "relative_humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
}
EXPECTED_UNITS = {"temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm", "wind_speed_10m": "km/h"}
MEASURES = tuple(RENAMES.values())

# Physically possible values per hour (not "normal" ones: a record cold snap is valid data).
RANGES = {
    "temperature_c": (-90.0, 60.0),
    "relative_humidity_pct": (0, 100),
    "precipitation_mm": (0.0, 500.0),
    "wind_speed_kmh": (0.0, 500.0),
}

SOURCE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm"
LINEAGE = ("_batch_id", "_source_file", "_ingested_at")
SILVER_COLUMNS = (*KEY, *MEASURES, *LINEAGE, "_merged_at")
QUARANTINE_COLUMNS = ("station_id", "observed_at_raw", "observed_at", *MEASURES, "rejection_reasons", *LINEAGE, "_quarantined_at")


def flatten_hourly(bronze: DataFrame) -> DataFrame:
    """One row per hour: bronze's `hourly` block holds parallel arrays (time[i] goes with temperature_2m[i] ...).

    If the arrays have different lengths, the missing values come out as null and `array_lengths_match` is false, so
    the rows are rejected instead of silently pairing a value with the wrong hour.
    """
    lengths_match = F.lit(True)
    for variable in RENAMES:
        lengths_match = lengths_match & (F.size(f"hourly.{variable}") == F.size("hourly.time"))
    exploded = bronze.select(
        "*",
        lengths_match.alias("array_lengths_match"),
        F.posexplode("hourly.time").alias("hour_index", "observed_at_raw"),
    )
    # F.get: 0-based, and null (not an error) past the end of a shorter array.
    return exploded.select(
        "station_id",
        "observed_at_raw",
        "utc_offset_seconds",
        *[F.get(F.col(f"hourly.{variable}"), F.col("hour_index")).alias(column) for variable, column in RENAMES.items()],
        *[F.col(f"hourly_units.{variable}").alias(f"unit_{variable}") for variable in RENAMES],
        "array_lengths_match",
        *LINEAGE,
    )


def add_observed_at(df: DataFrame) -> DataFrame:
    """`2026-09-20T13:00` in the response's time zone -> a UTC timestamp. Unparseable -> null (then rejected)."""
    local = F.try_to_timestamp(F.col("observed_at_raw"), F.lit(SOURCE_TIME_FORMAT))
    return df.withColumn("observed_at", F.timestamp_seconds(F.unix_seconds(local) - F.col("utc_offset_seconds")))


def _reason(condition: Column, reason: str) -> Column:
    return F.when(condition, F.lit(reason))


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Adds `rejection_reasons`: every rule the row breaks, or an empty array when it's valid.

    Every rule is written so that a null can't make it pass: `null < 0` is null, not true, so missing values are their
    own rule and range checks only run on present values. Units are compared null-safely (`<=>`), so a missing unit
    is a rejection too.
    """
    rules = [
        _reason(F.col("station_id").isNull(), "missing_station_id"),
        _reason(F.col("observed_at").isNull(), "invalid_observed_at"),
        _reason(~F.col("array_lengths_match"), "array_lengths_differ"),
    ]
    for column in MEASURES:
        low, high = RANGES[column]
        rules.append(_reason(F.col(column).isNull(), f"missing_{column}"))
        rules.append(_reason(F.col(column).isNotNull() & ~F.col(column).between(low, high), f"out_of_range_{column}"))
    for variable, unit in EXPECTED_UNITS.items():
        rules.append(_reason(~F.col(f"unit_{variable}").eqNullSafe(F.lit(unit)), f"unexpected_unit_{RENAMES[variable]}"))
    return df.withColumn("rejection_reasons", F.filter(F.array(*rules), lambda reason: reason.isNotNull()))


def split_valid_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Every row goes to exactly one side: size() of the reasons array is never null."""
    is_valid = F.size("rejection_reasons") == 0
    return df.where(is_valid), df.where(~is_valid)


def dedup_latest(valid: DataFrame) -> DataFrame:
    """One row per (station_id, observed_at). The most recently ingested wins; ties are broken on batch and file, so
    the result never depends on how Spark happened to order the rows."""
    latest_first = Window.partitionBy(*KEY).orderBy(F.desc("_ingested_at"), F.desc("_batch_id"), F.desc("_source_file"))
    return valid.withColumn("_rank", F.row_number().over(latest_first)).where("_rank = 1").drop("_rank")


def reconcile(input_rows: int, valid_rows: int, rejected_rows: int, unique_rows: int) -> dict[str, int]:
    """Every input row is either rejected, or valid and then kept or dropped as a duplicate. Raises otherwise."""
    counts = {
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "unique_valid_rows": unique_rows,
        "duplicates_dropped": valid_rows - unique_rows,
    }
    if input_rows != valid_rows + rejected_rows or unique_rows > valid_rows or unique_rows < 0:
        raise ValueError(f"rows don't reconcile: input must equal valid + rejected, and unique can't exceed valid: {counts}")
    return counts


def to_silver(unique: DataFrame) -> DataFrame:
    return unique.withColumn("_merged_at", F.current_timestamp()).select(*SILVER_COLUMNS)


def to_quarantine(rejected: DataFrame) -> DataFrame:
    return rejected.withColumn("_quarantined_at", F.current_timestamp()).select(*QUARANTINE_COLUMNS)


def classify_changes(updates: DataFrame, current: DataFrame) -> DataFrame:
    """Adds `change`: `new` (key not in silver), `changed` (different values, from data at least as recent), or
    `unchanged` (same values, or older data that must not overwrite newer).

    Only new and changed rows are sent to the MERGE. With copy-on-write, Iceberg rewrites every data file that holds a
    *matched* key, even when the WHEN MATCHED condition is false for all of them: a rerun would rewrite the whole
    partition and commit a snapshot while changing nothing (measured locally, see docs/PLAN.md step 3).
    """
    existing = current.select(*[F.col(k).alias(f"_current_{k}") for k in KEY], *[F.col(c).alias(f"_current_{c}") for c in (*MEASURES, "_ingested_at")])
    joined = updates.join(existing, [F.col(k) == F.col(f"_current_{k}") for k in KEY], "left")
    values_differ = F.lit(False)
    for column in MEASURES:
        values_differ = values_differ | ~F.col(column).eqNullSafe(F.col(f"_current_{column}"))
    change = (
        F.when(F.col("_current_station_id").isNull(), "new")
        .when(values_differ & (F.col("_ingested_at") >= F.col("_current__ingested_at")), "changed")
        .otherwise("unchanged")
    )
    return joined.withColumn("change", change).select(*updates.columns, "change")


def change_counts(classified: DataFrame) -> dict[str, int]:
    counts = {row["change"]: row["count"] for row in classified.groupBy("change").count().collect()}
    return {change: counts.get(change, 0) for change in ("new", "changed", "unchanged")}


def rows_to_merge(classified: DataFrame) -> DataFrame:
    return classified.where(F.col("change") != "unchanged").drop("change")
