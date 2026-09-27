"""Data quality checks for gold, run after computing and before writing: a failing check raises, so nothing is published.

Every check is computed in one aggregation pass and reported by name, so a failure says which rule broke and by how
much, not just that the job failed.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def gold_checks(gold: DataFrame, silver_readings: DataFrame) -> list[Check]:
    """Checks agg_readings_daily against itself and against the silver readings it was built from.

    Conditions test nulls explicitly: `count_if(x <= 0)` would skip a null x and let it pass.
    """
    g = gold.agg(
        F.count("*").alias("rows"),
        (F.count("*") - F.countDistinct("station_id", "reading_date")).alias("duplicate_keys"),
        F.count_if(
            F.col("station_id").isNull() | F.col("reading_date").isNull() | F.col("hours_observed").isNull()
            | F.col("temperature_min_c").isNull() | F.col("temperature_avg_c").isNull() | F.col("temperature_max_c").isNull()
        ).alias("null_required"),
        F.count_if(~F.col("hours_observed").between(1, 24)).alias("hours_out_of_range"),
        F.count_if(
            (F.col("temperature_min_c") > F.col("temperature_avg_c") + 1e-9) | (F.col("temperature_avg_c") > F.col("temperature_max_c") + 1e-9)
        ).alias("temperature_disorder"),
        F.count_if(F.col("population").isNull() | (F.col("population") <= 0)).alias("missing_population"),
        F.coalesce(F.sum("hours_observed"), F.lit(0)).alias("hours_total"),
        F.coalesce(F.sum("precipitation_total_mm"), F.lit(0.0)).alias("precipitation_total"),
    ).first()
    s = silver_readings.agg(F.count("*").alias("rows"), F.coalesce(F.sum("precipitation_mm"), F.lit(0.0)).alias("precipitation")).first()

    return [
        Check("not_empty", g.rows > 0, f"{g.rows} rows"),
        Check("unique_key", g.duplicate_keys == 0, f"{g.duplicate_keys} duplicate (station_id, reading_date)"),
        Check("required_not_null", g.null_required == 0, f"{g.null_required} rows with a null key, hour count or temperature"),
        Check("hours_in_range", g.hours_out_of_range == 0, f"{g.hours_out_of_range} rows outside 1-24 hours"),
        Check("temperature_order", g.temperature_disorder == 0, f"{g.temperature_disorder} rows where min <= avg <= max fails"),
        Check("population_found", g.missing_population == 0, f"{g.missing_population} station-days without a population count on or before them"),
        Check("hours_reconcile", g.hours_total == s.rows, f"gold counts {g.hours_total} hours, silver has {s.rows} readings"),
        Check("precipitation_reconciles", abs(g.precipitation_total - s.precipitation) < 1e-6, f"gold {g.precipitation_total:.3f} mm vs silver {s.precipitation:.3f} mm"),
    ]


def raise_if_any_failed(checks: list[Check]) -> None:
    if failed := [c for c in checks if not c.passed]:
        raise DataQualityError("data quality checks failed, nothing was published: " + "; ".join(f"{c.name}: {c.detail}" for c in failed))
