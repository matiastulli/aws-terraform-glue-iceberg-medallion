-- Population statements that failed validation (undated, invalid or deprecated), with every rule they broke.
-- A log per batch: a rerun of the same batch changes nothing, a later batch with the same bad statement adds a row.
CREATE TABLE `${catalog}`.`${silver_db}`.populations_quarantine (
  station_id        STRING,
  point_in_time_raw STRING    COMMENT 'The source date string, kept because it may be missing or not parse',
  population_raw    STRING    COMMENT 'The source value as a string, kept because it may not parse',
  rank_raw          STRING    COMMENT 'Wikidata statement rank URI',
  reference_date    DATE,
  population        BIGINT,
  rejection_reasons ARRAY<STRING> NOT NULL COMMENT 'Every rule the statement broke, e.g. missing_reference_date',
  _batch_id         STRING    NOT NULL,
  _source_file      STRING    NOT NULL,
  _ingested_at      TIMESTAMP NOT NULL,
  _quarantined_at   TIMESTAMP NOT NULL
)
USING iceberg
COMMENT 'Population statements rejected by clean_populations, with their rejection_reasons'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
