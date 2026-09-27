-- Hourly weather readings: one row per (station_id, observed_at), typed, validated and deduplicated.
-- Written only by clean_readings with a MERGE on that key, so reruns and duplicate loads change nothing.
-- Partitioned by day (queries filter on dates); sorted by the key within each file, so a station-hour lookup reads
-- few row groups. copy-on-write is Iceberg's default, made explicit: the MERGE is insert-mostly (new hours), so it
-- rarely rewrites a file, and readers never have to apply delete files.
CREATE TABLE `${catalog}`.`${silver_db}`.readings (
  station_id            STRING    NOT NULL COMMENT 'Weather station, from config/sources.toml',
  observed_at           TIMESTAMP NOT NULL COMMENT 'Start of the hour the reading covers (UTC)',
  temperature_c         DOUBLE    NOT NULL COMMENT 'Air temperature at 2 m, degrees Celsius',
  relative_humidity_pct INT       NOT NULL COMMENT 'Relative humidity at 2 m, percent',
  precipitation_mm      DOUBLE    NOT NULL COMMENT 'Precipitation over the hour, millimetres',
  wind_speed_kmh        DOUBLE    NOT NULL COMMENT 'Wind speed at 10 m, km/h',
  _batch_id             STRING    NOT NULL COMMENT 'Bronze batch the current values came from',
  _source_file          STRING    NOT NULL COMMENT 'Raw file the current values came from',
  _ingested_at          TIMESTAMP NOT NULL COMMENT 'When bronze loaded the current values; older data never overwrites newer',
  _merged_at            TIMESTAMP NOT NULL COMMENT 'When the MERGE last inserted or updated the row'
)
USING iceberg
PARTITIONED BY (days(observed_at))
COMMENT 'Hourly weather readings per station, deduplicated on (station_id, observed_at)'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.merge.mode' = 'copy-on-write',
  'write.update.mode' = 'copy-on-write',
  'write.delete.mode' = 'copy-on-write'
);

ALTER TABLE `${catalog}`.`${silver_db}`.readings WRITE ORDERED BY station_id, observed_at;
