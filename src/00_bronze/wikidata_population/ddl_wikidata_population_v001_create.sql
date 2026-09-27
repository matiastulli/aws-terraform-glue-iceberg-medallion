-- Raw Wikidata SPARQL responses: one row per station and run, as the endpoint returns them.
-- The response mirrors SPARQL JSON results: `head.vars` names the query variables and `results.bindings` holds one
-- entry per population statement, each value an RDF term {datatype, type, value} with the value as a string
-- (raw stays raw: silver types it). station_id comes from the raw file name; the _ columns are ingestion metadata.
CREATE TABLE `${catalog}`.`${bronze_db}`.wikidata_population (
  head         STRUCT<vars: ARRAY<STRING>>,
  results      STRUCT<bindings: ARRAY<STRUCT<
                 population:    STRUCT<datatype: STRING, `type`: STRING, value: STRING>,
                 point_in_time: STRUCT<datatype: STRING, `type`: STRING, value: STRING>,
                 `rank`:        STRUCT<`type`: STRING, value: STRING>
               >>> COMMENT 'One binding per population (P1082) statement, with its point in time (P585) and rank',
  station_id   STRING COMMENT 'From the raw file name <station_id>.json (config/sources.toml)',
  _batch_id    STRING COMMENT 'One per load: the Step Functions execution name',
  _ingested_at TIMESTAMP COMMENT 'When the load ran (UTC)',
  _source_file STRING COMMENT 'Raw file the row was read from'
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
COMMENT 'Raw Wikidata population statements per station, one row per station and run, append-only'
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
