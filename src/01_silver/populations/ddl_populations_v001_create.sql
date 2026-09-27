-- City population per station over time: one row per (station_id, reference_date), from Wikidata's dated counts.
-- Written only by clean_populations with a MERGE on that key. Gold uses the latest reference_date on or before a day
-- (an as-of join). Small and slowly changing, so not partitioned.
CREATE TABLE `${catalog}`.`${silver_db}`.populations (
  station_id        STRING    NOT NULL COMMENT 'Weather station, from config/sources.toml',
  reference_date    DATE      NOT NULL COMMENT 'The date the count refers to (Wikidata point in time, P585)',
  population        BIGINT    NOT NULL COMMENT 'People living in the city (Wikidata P1082)',
  is_preferred_rank BOOLEAN   NOT NULL COMMENT 'Wikidata marks its current best value preferred; older counts are normal rank',
  _batch_id         STRING    NOT NULL COMMENT 'Bronze batch the current values came from',
  _source_file      STRING    NOT NULL COMMENT 'Raw file the current values came from',
  _ingested_at      TIMESTAMP NOT NULL COMMENT 'When bronze loaded the current values; older data never overwrites newer',
  _merged_at        TIMESTAMP NOT NULL COMMENT 'When the MERGE last inserted or updated the row'
)
USING iceberg
COMMENT 'Population per station and reference date, deduplicated on (station_id, reference_date)'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.merge.mode' = 'copy-on-write'
);

ALTER TABLE `${catalog}`.`${silver_db}`.populations WRITE ORDERED BY station_id, reference_date;
