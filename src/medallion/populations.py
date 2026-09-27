"""Silver populations: one row per (station_id, reference_date), from Wikidata's SPARQL responses in bronze.

Each bronze row is one station's SPARQL response; `results.bindings` holds one entry per population statement, each
value an RDF term {type, value, datatype}. Values stay strings in bronze (raw stays raw) and are typed here.

    flatten_bindings      -> one row per statement, with the raw strings kept for the quarantine
    add_rejection_reasons -> undated, non-numeric, non-positive or fractional counts, and deprecated statements

Gold picks, for each day, the latest reference_date on or before it (an as-of join).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from medallion.silver import LINEAGE, collect_reasons

KEY = ("station_id", "reference_date")
# is_preferred_rank is a value: when a new census arrives, Wikidata demotes the old one to normal rank.
VALUES = ("population", "is_preferred_rank")
RANK_PREFIX = "http://wikiba.se/ontology#"

SILVER_COLUMNS = (*KEY, *VALUES, *LINEAGE, "_merged_at")
QUARANTINE_COLUMNS = (
    "station_id", "point_in_time_raw", "population_raw", "rank_raw", "reference_date", "population",
    "rejection_reasons", *LINEAGE, "_quarantined_at",
)
QUARANTINE_MATCH = ("station_id", "point_in_time_raw", "population_raw", "rank_raw", "_batch_id")


def flatten_bindings(bronze: DataFrame) -> DataFrame:
    """One row per population statement, typed. Unparseable values become null here and are rejected by the rules."""
    statements = bronze.select("station_id", *LINEAGE, F.explode("results.bindings").alias("binding"))
    population = F.expr("try_cast(binding.population.value AS DECIMAL(38, 6))")
    # Wikidata dates are xsd:dateTime (2022-01-01T00:00:00Z); a census count is dated by its day.
    reference_date = F.to_date(F.try_to_timestamp(F.substring("binding.point_in_time.value", 1, 10), F.lit("yyyy-MM-dd")))
    return statements.select(
        "station_id",
        F.col("binding.point_in_time.value").alias("point_in_time_raw"),
        F.col("binding.population.value").alias("population_raw"),
        F.col("binding.rank.value").alias("rank_raw"),
        reference_date.alias("reference_date"),
        population.alias("population_decimal"),
        F.when(population == F.floor(population), population.cast("bigint")).alias("population"),
        (F.col("binding.rank.value") == F.lit(f"{RANK_PREFIX}PreferredRank")).alias("is_preferred_rank"),
        *LINEAGE,
    )


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Nulls are tested explicitly: a missing value must never pass as valid because `null <= 0` is null."""
    rules: list[tuple[Column, str]] = [
        (F.col("station_id").isNull(), "missing_station_id"),
        (F.col("point_in_time_raw").isNull(), "missing_reference_date"),
        (F.col("point_in_time_raw").isNotNull() & F.col("reference_date").isNull(), "invalid_reference_date"),
        (F.col("population_raw").isNull(), "missing_population"),
        (F.col("population_raw").isNotNull() & F.col("population_decimal").isNull(), "invalid_population"),
        (F.col("population_decimal").isNotNull() & (F.col("population_decimal") <= 0), "non_positive_population"),
        (F.col("population_decimal").isNotNull() & F.col("population").isNull(), "fractional_population"),
        # Wikidata keeps values it knows are wrong, marked deprecated, instead of deleting them.
        (F.col("rank_raw").eqNullSafe(F.lit(f"{RANK_PREFIX}DeprecatedRank")), "deprecated_rank"),
    ]
    return collect_reasons(df, rules)


def preference() -> list[Column]:
    """Several statements for the same date (e.g. a census and an estimate): Wikidata's preferred one wins.
    A function, not a constant: building a Column needs an active Spark session, which doesn't exist at import time."""
    return [F.desc("is_preferred_rank")]


def to_silver(unique: DataFrame) -> DataFrame:
    return unique.withColumn("_merged_at", F.current_timestamp()).select(*SILVER_COLUMNS)


def to_quarantine(rejected: DataFrame) -> DataFrame:
    return rejected.withColumn("_quarantined_at", F.current_timestamp()).select(*QUARANTINE_COLUMNS)
