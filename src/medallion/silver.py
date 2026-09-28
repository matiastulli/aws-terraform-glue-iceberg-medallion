"""Silver mechanics shared by every entity (readings, populations): what makes a clean job safe to rerun.

An entity module (readings.py, populations.py) flattens and validates its bronze rows into a DataFrame with a
`rejection_reasons` array. From there every entity goes the same way:

    split_valid_and_rejected -> every row on exactly one side
    dedup_latest             -> one row per key, with a deterministic winner
    reconcile                -> every input row accounted for, or the job fails before writing
    batch_bounds / between_sql -> bound silver to the batch's key range, so reading it doesn't grow with its history
    classify_changes         -> new / changed / unchanged against the silver table
    rows_to_merge            -> only new and changed rows reach the MERGE (see classify_changes for why)
    merge_sql / quarantine_merge_sql -> the MERGE statements, which the Glue job runs
"""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

LINEAGE = ("_batch_id", "_source_file", "_ingested_at")


def collect_reasons(df: DataFrame, rules: Sequence[tuple[Column, str]]) -> DataFrame:
    """Adds `rejection_reasons`: the name of every (condition, name) rule whose condition is true, [] when valid.

    A condition that is null (e.g. `null < 0`) doesn't count as broken, so entity rules must test nulls explicitly.
    """
    return df.withColumn(
        "rejection_reasons",
        F.filter(F.array(*[F.when(condition, F.lit(name)) for condition, name in rules]), lambda reason: reason.isNotNull()),
    )


def split_valid_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Every row goes to exactly one side: size() of the reasons array is never null."""
    is_valid = F.size("rejection_reasons") == 0
    return df.where(is_valid), df.where(~is_valid)


def dedup_latest(valid: DataFrame, key: Sequence[str], prefer: Sequence[Column] = ()) -> DataFrame:
    """One row per key. `prefer` orders candidates first (e.g. Wikidata's preferred rank); then the most recently
    ingested wins, with ties broken on batch and file, so the winner never depends on how Spark ordered the rows."""
    order = [*prefer, F.desc("_ingested_at"), F.desc("_batch_id"), F.desc("_source_file")]
    ranked = valid.withColumn("_rank", F.row_number().over(Window.partitionBy(*key).orderBy(*order)))
    return ranked.where("_rank = 1").drop("_rank")


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


def batch_bounds(updates: DataFrame, column: str) -> tuple[str, str] | None:
    """The batch's min and max of a key column, as SQL literals of the column's type; None when the batch is empty.

    silver can only match a key the batch holds, so filtering silver to this range changes no result, and it keeps
    the classify_changes read and the MERGE's target scan to the batch's partitions instead of the whole history.
    The bounds must be literals: Spark and Iceberg prune files on constant filters, not on values that come from the
    other side of a join. Casting to string and back runs in the session time zone, so a timestamp round-trips exactly.
    """
    sql_type = updates.schema[column].dataType.simpleString().upper()
    low, high = updates.agg(F.min(column).cast("string"), F.max(column).cast("string")).first()
    return None if low is None else (f"CAST('{low}' AS {sql_type})", f"CAST('{high}' AS {sql_type})")


def between_sql(column: str, bounds: tuple[str, str] | None) -> str:
    """`column BETWEEN low AND high`; FALSE for an empty batch, which has nothing to match."""
    return "FALSE" if bounds is None else f"{column} BETWEEN {bounds[0]} AND {bounds[1]}"


def classify_changes(updates: DataFrame, current: DataFrame, key: Sequence[str], values: Sequence[str]) -> DataFrame:
    """Adds `change`: `new` (key not in silver), `changed` (different values, from data at least as recent), or
    `unchanged` (same values, or older data that must not overwrite newer).

    Only new and changed rows are sent to the MERGE. With copy-on-write, Iceberg rewrites every data file that holds a
    *matched* key, even when the WHEN MATCHED condition is false for all of them: a rerun would rewrite the whole
    partition and commit a snapshot while changing nothing (measured, see docs/PLAN.md step 3).
    """
    existing = current.select(*[F.col(c).alias(f"_current_{c}") for c in (*key, *values, "_ingested_at")])
    joined = updates.join(existing, [F.col(k) == F.col(f"_current_{k}") for k in key], "left")
    values_differ = F.lit(False)
    for column in values:
        values_differ = values_differ | ~F.col(column).eqNullSafe(F.col(f"_current_{column}"))
    change = (
        F.when(F.col(f"_current_{key[0]}").isNull(), "new")
        .when(values_differ & (F.col("_ingested_at") >= F.col("_current__ingested_at")), "changed")
        .otherwise("unchanged")
    )
    return joined.withColumn("change", change).select(*updates.columns, "change")


def change_counts(classified: DataFrame) -> dict[str, int]:
    counts = {row["change"]: row["count"] for row in classified.groupBy("change").count().collect()}
    return {change: counts.get(change, 0) for change in ("new", "changed", "unchanged")}


def rows_to_merge(classified: DataFrame) -> DataFrame:
    return classified.where(F.col("change") != "unchanged").drop("change")


def merge_sql(table: str, source_view: str, key: Sequence[str], values: Sequence[str], target_filter: str | None = None) -> str:
    """MERGE on the key: insert new keys; update a match only when its values differ and the incoming data isn't
    older, so a late backfill can't overwrite newer data. The conditions repeat classify_changes, so the statement
    stays correct on its own (e.g. if another writer committed in between).

    `target_filter` (a predicate on `t.` with literal values, see batch_bounds) is added to ON, where Iceberg pushes
    it into the target scan: without it the MERGE reads every data file of the table to look for matches."""
    on = " AND ".join([*(f"t.{k} = s.{k}" for k in key), *([target_filter] if target_filter else [])])
    values_differ = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in values)
    return (
        f"MERGE INTO {table} t\nUSING {source_view} s\nON {on}\n"
        f"WHEN MATCHED AND s._ingested_at >= t._ingested_at AND ({values_differ}) THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


def quarantine_merge_sql(table: str, source_view: str, match_on: Sequence[str]) -> str:
    """Insert-only MERGE for a quarantine log. `<=>` (null-safe equality): a reject may have null values, and
    `null = null` never matches, so a rerun would insert the same reject again."""
    on = " AND ".join(f"t.{c} <=> s.{c}" for c in match_on)
    return f"MERGE INTO {table} t\nUSING {source_view} s\nON {on}\nWHEN NOT MATCHED THEN INSERT *"


def new_rejects(rejects: DataFrame, quarantine: DataFrame, match_on: Sequence[str]) -> DataFrame:
    """Rejects not yet in the quarantine table (null-safe), so an empty quarantine MERGE can be skipped."""
    condition = [rejects[c].eqNullSafe(quarantine[c]) for c in match_on]
    return rejects.join(quarantine.select(*match_on), condition, "left_anti")
