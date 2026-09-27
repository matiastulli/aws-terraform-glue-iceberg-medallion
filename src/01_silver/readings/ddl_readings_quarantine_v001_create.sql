-- Readings that failed validation, with every rule they broke. A log per batch: a rerun of the same batch changes
-- nothing (MERGE on station_id, observed_at_raw, _batch_id), and a later batch with the same bad reading adds a row.
-- Values are nullable here: missing or unparseable values are exactly what lands in this table.
CREATE TABLE `${catalog}`.`${silver_db}`.readings_quarantine (
  station_id            STRING,
  observed_at_raw       STRING    COMMENT 'The source time string, kept because it may not parse',
  observed_at           TIMESTAMP COMMENT 'Parsed from observed_at_raw (UTC), null when it could not be',
  temperature_c         DOUBLE,
  relative_humidity_pct INT,
  precipitation_mm      DOUBLE,
  wind_speed_kmh        DOUBLE,
  rejection_reasons     ARRAY<STRING> NOT NULL COMMENT 'Every rule the reading broke, e.g. missing_temperature_c',
  _batch_id             STRING    NOT NULL,
  _source_file          STRING    NOT NULL,
  _ingested_at          TIMESTAMP NOT NULL,
  _quarantined_at       TIMESTAMP NOT NULL
)
USING iceberg
COMMENT 'Readings rejected by clean_readings, with their rejection_reasons'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
