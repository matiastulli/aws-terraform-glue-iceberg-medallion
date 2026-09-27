-- Daily weather per station with the population living there that day: one row per (station_id, reading_date).
-- Rebuilt in full by build_reading_metrics from silver readings and populations, published only after the data
-- quality checks pass, and not rewritten when nothing changed. Small, so not partitioned; sorted by the key.
CREATE TABLE `${catalog}`.`${gold_db}`.agg_readings_daily (
  station_id                STRING    NOT NULL COMMENT 'Weather station, from config/sources.toml',
  reading_date              DATE      NOT NULL COMMENT 'UTC day',
  hours_observed            INT       NOT NULL COMMENT 'Hourly readings that day in silver (24 = complete)',
  is_complete_day           BOOLEAN   NOT NULL COMMENT 'hours_observed = 24',
  temperature_min_c         DOUBLE    NOT NULL,
  temperature_max_c         DOUBLE    NOT NULL,
  temperature_avg_c         DOUBLE    NOT NULL,
  temperature_avg_7d_c      DOUBLE    NOT NULL COMMENT 'Average of daily averages over the 7 calendar days ending this day (fewer if days are missing)',
  temperature_change_c      DOUBLE             COMMENT 'Change of the daily average vs the previous calendar day; null when that day is missing',
  relative_humidity_avg_pct DOUBLE    NOT NULL,
  precipitation_total_mm    DOUBLE    NOT NULL,
  wind_speed_max_kmh        DOUBLE    NOT NULL,
  population                BIGINT    NOT NULL COMMENT 'People living in the city: the latest count on or before reading_date',
  population_reference_date DATE      NOT NULL COMMENT 'Date of that count (e.g. the 2022 census)',
  _built_at                 TIMESTAMP NOT NULL COMMENT 'When build_reading_metrics published this version'
)
USING iceberg
COMMENT 'Daily weather aggregates per station, with the population as of each day'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE `${catalog}`.`${gold_db}`.agg_readings_daily WRITE ORDERED BY station_id, reading_date;
