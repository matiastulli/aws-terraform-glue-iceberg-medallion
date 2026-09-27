"""Silver readings: one typed, validated row per station-hour, from bronze's one-row-per-station-day API responses.

    flatten_hourly        -> one row per hour, source columns renamed to the column convention
    add_observed_at       -> the source's local time string -> a UTC timestamp
    add_rejection_reasons -> every rule that fails, as a list (empty = valid)

The rest (split, dedup, reconcile, MERGE) is shared with every silver entity, in silver.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from medallion.silver import LINEAGE, collect_reasons

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
SILVER_COLUMNS = (*KEY, *MEASURES, *LINEAGE, "_merged_at")
QUARANTINE_COLUMNS = ("station_id", "observed_at_raw", "observed_at", *MEASURES, "rejection_reasons", *LINEAGE, "_quarantined_at")
# What identifies a reject in the quarantine log: the same bad reading from the same batch is recorded once.
QUARANTINE_MATCH = ("station_id", "observed_at_raw", "_batch_id")


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


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Every rule is written so that a null can't make it pass: `null < 0` is null, not true, so missing values are
    their own rule and range checks only run on present values. Units are compared null-safely (`<=>`), so a missing
    unit is a rejection too."""
    rules = [
        (F.col("station_id").isNull(), "missing_station_id"),
        (F.col("observed_at").isNull(), "invalid_observed_at"),
        (~F.col("array_lengths_match"), "array_lengths_differ"),
    ]
    for column in MEASURES:
        low, high = RANGES[column]
        rules.append((F.col(column).isNull(), f"missing_{column}"))
        rules.append((F.col(column).isNotNull() & ~F.col(column).between(low, high), f"out_of_range_{column}"))
    for variable, unit in EXPECTED_UNITS.items():
        rules.append((~F.col(f"unit_{variable}").eqNullSafe(F.lit(unit)), f"unexpected_unit_{RENAMES[variable]}"))
    return collect_reasons(df, rules)


def to_silver(unique: DataFrame) -> DataFrame:
    return unique.withColumn("_merged_at", F.current_timestamp()).select(*SILVER_COLUMNS)


def to_quarantine(rejected: DataFrame) -> DataFrame:
    return rejected.withColumn("_quarantined_at", F.current_timestamp()).select(*QUARANTINE_COLUMNS)
