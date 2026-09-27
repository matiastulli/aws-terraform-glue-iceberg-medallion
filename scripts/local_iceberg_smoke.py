"""Step 0 check: PySpark + Apache Iceberg on the laptop.

Creates a small Iceberg table with hidden partitioning, upserts into it with MERGE,
and reads an older snapshot back (time travel). Everything lives in ./local-warehouse
(git-ignored), under a Hadoop catalog named `local`.

    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    .venv/bin/python scripts/local_iceberg_smoke.py
"""

from pyspark.sql import SparkSession

# Same Iceberg version as AWS Glue 5.1. The jar is downloaded from Maven on first run.
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"

spark = (
    SparkSession.builder.appName("local-iceberg-smoke")
    .master("local[2]")
    .config("spark.jars.packages", ICEBERG_RUNTIME)
    # Enables MERGE INTO, ALTER TABLE ... WRITE ORDERED BY and the Iceberg procedures.
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "local-warehouse")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("DROP TABLE IF EXISTS local.smoke.readings PURGE")
# Hidden partitioning: partitioned by the day of observed_at, with no extra date column.
# A filter on observed_at alone prunes partitions.
spark.sql("""
    CREATE TABLE local.smoke.readings (
        station_id     STRING,
        observed_at    TIMESTAMP,
        temperature_c  DOUBLE
    )
    USING iceberg
    PARTITIONED BY (days(observed_at))
""")

spark.sql("""
    INSERT INTO local.smoke.readings VALUES
        ('buenos_aires', TIMESTAMP '2026-01-01 00:00:00', 24.1),
        ('buenos_aires', TIMESTAMP '2026-01-01 01:00:00', 23.6),
        ('cordoba',      TIMESTAMP '2026-01-01 00:00:00', 21.0)
""")
first_snapshot = spark.sql(
    "SELECT snapshot_id FROM local.smoke.readings.snapshots ORDER BY committed_at"
).first()[0]

# Upsert keyed by (station_id, observed_at): one correction and one new reading.
spark.createDataFrame(
    [("buenos_aires", "2026-01-01 01:00:00", 23.9), ("cordoba", "2026-01-02 00:00:00", 19.5)],
    "station_id STRING, observed_at STRING, temperature_c DOUBLE",
).selectExpr("station_id", "CAST(observed_at AS TIMESTAMP) AS observed_at", "temperature_c").createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO local.smoke.readings t
    USING updates s
    ON t.station_id = s.station_id AND t.observed_at = s.observed_at
    WHEN MATCHED THEN UPDATE SET t.temperature_c = s.temperature_c
    WHEN NOT MATCHED THEN INSERT *
""")

now = spark.table("local.smoke.readings").count()
then = spark.sql(f"SELECT * FROM local.smoke.readings VERSION AS OF {first_snapshot}").count()
partitions = spark.sql("SELECT partition FROM local.smoke.readings.partitions").count()
print(f"rows now: {now}, rows at first snapshot: {then}, partitions: {partitions}")
assert (now, then, partitions) == (4, 3, 2), "unexpected Iceberg behaviour"
spark.sql("SELECT operation, summary['added-records'] AS added FROM local.smoke.readings.snapshots").show()
print("OK: Iceberg MERGE, hidden partitioning and time travel work locally")
