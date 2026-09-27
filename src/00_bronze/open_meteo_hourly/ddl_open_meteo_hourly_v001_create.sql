-- Raw Open-Meteo archive responses: one row per station and day, as the API returns them.
-- Source columns keep their names and nesting (hourly is a block of parallel arrays, flattened in silver).
-- station_id comes from the raw file name; the _ columns are ingestion metadata.
-- Partitioned by load day, the way bronze is read: append-only, one _batch_id per load.
CREATE TABLE `${catalog}`.`${bronze_db}`.open_meteo_hourly (
  latitude              DOUBLE COMMENT 'Grid cell latitude chosen by the API, not the requested one',
  longitude             DOUBLE COMMENT 'Grid cell longitude chosen by the API',
  generationtime_ms     DOUBLE,
  utc_offset_seconds    INT,
  timezone              STRING,
  timezone_abbreviation STRING,
  elevation             DOUBLE COMMENT 'Metres',
  hourly_units          STRUCT<`time`: STRING, temperature_2m: STRING, relative_humidity_2m: STRING, precipitation: STRING, wind_speed_10m: STRING>,
  hourly                STRUCT<`time`: ARRAY<STRING>, temperature_2m: ARRAY<DOUBLE>, relative_humidity_2m: ARRAY<INT>, precipitation: ARRAY<DOUBLE>, wind_speed_10m: ARRAY<DOUBLE>>,
  station_id            STRING COMMENT 'From the raw file name <station_id>.json (config/sources.toml)',
  _batch_id             STRING COMMENT 'One per load: the Step Functions execution name',
  _ingested_at          TIMESTAMP COMMENT 'When the load ran (UTC)',
  _source_file          STRING COMMENT 'Raw file the row was read from'
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
COMMENT 'Raw Open-Meteo hourly archive responses, one row per station-day, append-only'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
