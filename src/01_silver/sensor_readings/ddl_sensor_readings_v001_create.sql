-- Live sensor readings: one row per (station_id, observed_at) at the minute, validated and deduplicated
-- (docs/PLAN.md step 9). Written only by clean_sensor_readings, a Structured Streaming job, with a MERGE on that key,
-- so SQS redeliveries, sensor resends and reruns change nothing. Late readings land at their own minute.
-- Partitioned by day: a sensor per station every minute is 4,320 rows a day, sorted by the key within each file.
CREATE TABLE `${catalog}`.`${silver_db}`.sensor_readings (
  station_id            STRING    NOT NULL COMMENT 'Weather station, from config/sources.toml',
  observed_at           TIMESTAMP NOT NULL COMMENT 'Minute the reading was taken (UTC)',
  temperature_c         DOUBLE    NOT NULL,
  relative_humidity_pct INT       NOT NULL,
  precipitation_mm      DOUBLE    NOT NULL,
  wind_speed_kmh        DOUBLE    NOT NULL,
  event_id              STRING    COMMENT 'Set by the sensor; a resend repeats it',
  _batch_id             STRING    NOT NULL COMMENT 'Bronze batch the current values came from (the consumer Lambda invocation)',
  _message_id           STRING    NOT NULL COMMENT 'SQS message the current values came from',
  _ingested_at          TIMESTAMP NOT NULL COMMENT 'When bronze appended the current values; older data never overwrites newer',
  _merged_at            TIMESTAMP NOT NULL COMMENT 'When the MERGE last inserted or updated the row'
)
USING iceberg
PARTITIONED BY (days(observed_at))
COMMENT 'Live sensor readings per station and minute, deduplicated on (station_id, observed_at)'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.merge.mode' = 'copy-on-write',
  'write.update.mode' = 'copy-on-write',
  'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE `${catalog}`.`${silver_db}`.sensor_readings WRITE ORDERED BY station_id, observed_at;
