-- Partition by month instead of by day (docs/PLAN.md step 7). At 72 rows per day, a daily partition is one ~4 KB
-- file; a month is closer to a sensible file size, and queries still prune on observed_at.
-- Metadata-only: existing files keep the day spec and stay readable; new writes use months. maintain_tables with
-- --rewrite_all true moves the old files to the new spec.
-- observed_at_day is the name Iceberg gave the days(observed_at) field in v001.
ALTER TABLE `${catalog}`.`${silver_db}`.readings REPLACE PARTITION FIELD observed_at_day WITH months(observed_at);
