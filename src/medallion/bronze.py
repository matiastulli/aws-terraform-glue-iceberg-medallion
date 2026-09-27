"""Bronze: raw files loaded as they are, plus the columns the loader adds.

The loader never infers a schema. It reads the raw JSON with the bronze table's own schema minus the added columns,
so the table's DDL decides how a file is read, and a changed source shows up as a contract or read failure, never as a
silently changed table.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# station_id comes from the file name (the API response doesn't carry it); the _ columns are pipeline metadata.
ADDED_COLUMNS = ("station_id", "_batch_id", "_ingested_at", "_source_file")
STATION_FILE = r"/([a-z][a-z0-9_]*)\.json$"


def source_schema(table_schema: StructType) -> StructType:
    """The schema to read raw files with: the bronze table's columns minus the ones the loader adds."""
    return StructType([field for field in table_schema.fields if field.name not in ADDED_COLUMNS])


def station_id_from_file(path: Column) -> Column:
    """`.../date=2026-09-20/buenos_aires.json` -> `buenos_aires`. A file not named <station_id>.json fails the job."""
    station_id = F.regexp_extract(path, STATION_FILE, 1)
    return F.when(station_id != "", station_id).otherwise(
        F.raise_error(F.concat(F.lit("raw file is not named <station_id>.json: "), path))
    )


def add_ingestion_metadata(df: DataFrame, batch_id: str, source_file: Column) -> DataFrame:
    """Adds station_id (from the file name) and the _ metadata. `source_file` is usually `_metadata.file_path`."""
    return (
        df.withColumn("station_id", station_id_from_file(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", source_file)
    )
