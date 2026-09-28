-- Sensor readings that failed validation, with every rule they broke. One row per rejected SQS delivery
-- (MERGE on _message_id), so a rerun of the stream quarantines nothing twice.
CREATE TABLE `${catalog}`.`${silver_db}`.sensor_readings_quarantine (
  station_id            STRING,
  observed_at_raw       STRING    COMMENT 'The sensor time string, kept because it may not parse',
  observed_at           TIMESTAMP COMMENT 'Parsed from observed_at_raw (UTC), null when it could not be',
  temperature_c         DOUBLE,
  relative_humidity_pct INT,
  precipitation_mm      DOUBLE,
  wind_speed_kmh        DOUBLE,
  rejection_reasons     ARRAY<STRING> NOT NULL COMMENT 'Every rule the reading broke, e.g. missing_temperature_c',
  event_id              STRING,
  _batch_id             STRING    NOT NULL,
  _message_id           STRING    NOT NULL,
  _ingested_at          TIMESTAMP NOT NULL,
  _quarantined_at       TIMESTAMP NOT NULL
)
USING iceberg
COMMENT 'Sensor readings rejected by clean_sensor_readings, with their rejection_reasons'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
