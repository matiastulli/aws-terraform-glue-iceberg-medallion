import datetime as dt

import pytest

from medallion.maintenance import Options, expire_cutoff, expire_snapshots_sql, procedure_table, remove_orphan_files_sql, rewrite_data_files_sql

NOW = dt.datetime(2026, 9, 28, 18, 30, 15, tzinfo=dt.timezone.utc)
TABLE = procedure_table("01_silver", "readings")


def test_a_sorted_table_is_compacted_with_the_sort_strategy():
    # binpack on a sorted table wrote 200 files out of 10 (docs/PLAN.md step 7); sort wrote 1.
    sorted_sql = rewrite_data_files_sql("glue_catalog", TABLE, "station_id ASC NULLS FIRST", rewrite_all=False)
    assert "strategy => 'sort'" in sorted_sql
    assert "strategy => 'binpack'" in rewrite_data_files_sql("glue_catalog", TABLE, None, rewrite_all=False)
    assert "table => '`01_silver`.readings'" in sorted_sql


def test_rewrite_all_is_opt_in():
    assert "'rewrite-all', 'true'" in rewrite_data_files_sql("glue_catalog", TABLE, None, rewrite_all=True)
    assert "rewrite-all" not in rewrite_data_files_sql("glue_catalog", TABLE, None, rewrite_all=False)


def test_expiry_and_orphan_cutoffs_are_utc_literals_counted_back_from_now():
    options = Options(expire_older_than_days=7, retain_last=5, orphan_older_than_days=3)

    assert expire_snapshots_sql("glue_catalog", TABLE, options, expire_cutoff(options, NOW, None)).endswith(
        "older_than => TIMESTAMP '2026-09-21 18:30:15', retain_last => 5)"
    )
    assert remove_orphan_files_sql("glue_catalog", TABLE, options, NOW).endswith(
        "older_than => TIMESTAMP '2026-09-25 18:30:15', dry_run => false)"
    )
    # A cutoff given in another time zone is the same instant in UTC.
    buenos_aires = NOW.astimezone(dt.timezone(dt.timedelta(hours=-3)))
    assert expire_snapshots_sql("glue_catalog", TABLE, options, buenos_aires) == expire_snapshots_sql("glue_catalog", TABLE, options, NOW)


def test_expiry_never_passes_the_snapshot_a_stream_last_consumed():
    # The stream's checkpoint needs its last snapshot to find the next ones; expiring it broke the stream (step 9).
    options = Options(expire_older_than_days=7)
    stream_behind = NOW - dt.timedelta(days=10)
    stream_current = NOW - dt.timedelta(minutes=5)

    assert expire_cutoff(options, NOW, stream_behind) == stream_behind
    assert expire_cutoff(options, NOW, stream_current) == NOW - dt.timedelta(days=7)
    assert expire_cutoff(Options(expire_older_than_days=0), NOW, stream_current) == stream_current
    assert expire_cutoff(options, NOW, None) == NOW - dt.timedelta(days=7)


@pytest.mark.parametrize(
    ("options", "problem"),
    [
        ({"retain_last": 0}, "retain_last must be at least 1"),
        ({"orphan_older_than_days": 0}, "orphan_older_than_days must be at least 1"),
        ({"expire_older_than_days": -1}, "can't be negative"),
    ],
)
def test_options_that_could_lose_data_are_refused(options, problem):
    # Orphans younger than a day may be files of a commit in progress; a table needs at least its current snapshot.
    with pytest.raises(ValueError, match=problem):
        Options(**options)
