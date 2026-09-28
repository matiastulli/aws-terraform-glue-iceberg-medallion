-- Messages that couldn't be appended to simulator_readings as sent: not JSON, an unknown field, or a value of the
-- wrong type. Kept with the raw body for a person to look at; a retry couldn't fix them, so they never go back to
-- the queue.
CREATE TABLE `${catalog}`.`${bronze_db}`.simulator_readings_quarantine (
  _message_id       STRING,
  body              STRING        COMMENT 'The message exactly as received',
  rejection_reasons ARRAY<STRING> COMMENT 'Every problem found, e.g. relative_humidity_pct is not int',
  _sent_at          TIMESTAMP,
  _batch_id         STRING,
  _ingested_at      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
COMMENT 'Sensor messages rejected by consume_sensor_readings, with their raw body and rejection_reasons'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
