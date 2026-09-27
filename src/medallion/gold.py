"""Gold: daily weather per station, with the population living there that day.

    daily_readings   -> one row per (station_id, reading_date) from silver readings
    add_trends       -> 7-day rolling average and change vs the previous day, by calendar, not by row
    with_population  -> the latest population count on or before each day (an as-of join)
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KEY = ("station_id", "reading_date")
COLUMNS = (
    *KEY,
    "hours_observed",
    "is_complete_day",
    "temperature_min_c",
    "temperature_max_c",
    "temperature_avg_c",
    "temperature_avg_7d_c",
    "temperature_change_c",
    "relative_humidity_avg_pct",
    "precipitation_total_mm",
    "wind_speed_max_kmh",
    "population",
    "population_reference_date",
    "_built_at",
)


def daily_readings(readings: DataFrame) -> DataFrame:
    """Silver's hourly readings -> one row per station and UTC day."""
    return (
        readings.withColumn("reading_date", F.to_date("observed_at"))
        .groupBy(*KEY)
        .agg(
            F.count("*").cast("int").alias("hours_observed"),
            F.min("temperature_c").alias("temperature_min_c"),
            F.max("temperature_c").alias("temperature_max_c"),
            F.avg("temperature_c").alias("temperature_avg_c"),
            F.avg("relative_humidity_pct").alias("relative_humidity_avg_pct"),
            F.sum("precipitation_mm").alias("precipitation_total_mm"),
            F.max("wind_speed_kmh").alias("wind_speed_max_kmh"),
        )
        .withColumn("is_complete_day", F.col("hours_observed") == 24)
    )


def add_trends(daily: DataFrame) -> DataFrame:
    """Window functions over each station's days, defined by the calendar rather than by row position.

    - temperature_avg_7d_c: average of the daily averages over the 7 calendar days ending that day. A range over the
      day number (unix_date), so a missing day shrinks the window instead of pulling in an 8th, older day.
    - temperature_change_c: vs the previous calendar day (LAG), null when that day is missing: the row before a gap
      is not "yesterday".
    """
    by_station = Window.partitionBy("station_id").orderBy(F.unix_date("reading_date"))
    previous_date = F.lag("reading_date").over(by_station)
    previous_avg = F.lag("temperature_avg_c").over(by_station)
    return daily.withColumn(
        "temperature_avg_7d_c", F.avg("temperature_avg_c").over(by_station.rangeBetween(-6, 0))
    ).withColumn(
        "temperature_change_c",
        F.when(F.date_add(previous_date, 1) == F.col("reading_date"), F.col("temperature_avg_c") - previous_avg),
    )


def with_population(daily: DataFrame, populations: DataFrame) -> DataFrame:
    """As-of join: each day gets the latest count dated on or before it (never a later census). A day before every
    count keeps a null population, which the data quality checks reject."""
    counts = populations.select(
        F.col("station_id").alias("_p_station_id"),
        F.col("reference_date").alias("population_reference_date"),
        "population",
    )
    joined = daily.join(
        counts,
        (F.col("station_id") == F.col("_p_station_id")) & (F.col("population_reference_date") <= F.col("reading_date")),
        "left",
    )
    latest_first = Window.partitionBy(*KEY).orderBy(F.col("population_reference_date").desc_nulls_last())
    return joined.withColumn("_rank", F.row_number().over(latest_first)).where("_rank = 1").drop("_rank", "_p_station_id")


def build_agg_readings_daily(readings: DataFrame, populations: DataFrame) -> DataFrame:
    return with_population(add_trends(daily_readings(readings)), populations).withColumn("_built_at", F.current_timestamp()).select(*COLUMNS)


def differs_from(new: DataFrame, current: DataFrame) -> bool:
    """True when the rebuilt table differs from the published one, ignoring _built_at. A full rebuild that changes
    nothing is then skipped, so reruns commit no snapshot (docs/PLAN.md step 3)."""
    columns = [c for c in COLUMNS if c != "_built_at"]
    a, b = new.select(*columns), current.select(*columns)
    return not (a.exceptAll(b).isEmpty() and b.exceptAll(a).isEmpty())
