-- Live sensor readings from the simulator, one row per SQS message, as the sensor sent them (docs/PLAN.md step 6).
-- Appended by the consume_sensor_readings Lambda with pyiceberg, one commit per Lambda batch. Duplicates and late
-- readings are expected here: SQS is at-least-once, sensors resend, and silver dedups on (station_id, observed_at).
-- Partitioned by load day like the other bronze tables.
CREATE TABLE `${catalog}`.`${bronze_db}`.simulator_readings (
  event_id              STRING    COMMENT 'Set by the sensor: a resend repeats it',
  station_id            STRING,
  observed_at           STRING    COMMENT 'Time string as the sensor sent it (UTC, ISO 8601), parsed in silver',
  temperature_c         DOUBLE,
  relative_humidity_pct INT,
  precipitation_mm      DOUBLE,
  wind_speed_kmh        DOUBLE,
  _message_id           STRING    COMMENT 'SQS message id: a redelivery of the same message repeats it',
  _sent_at              TIMESTAMP COMMENT 'When SQS received the message; minus observed_at, how late it was',
  _batch_id             STRING    COMMENT 'The Lambda invocation that appended it (one Iceberg commit)',
  _ingested_at          TIMESTAMP COMMENT 'When the Lambda appended it (UTC)'
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
COMMENT 'Simulated live sensor readings from SQS, one row per message, append-only'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
