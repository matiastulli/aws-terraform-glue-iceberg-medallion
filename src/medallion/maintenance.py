"""Iceberg table maintenance (docs/PLAN.md step 7): the procedure calls the maintain_tables job runs, and the limits
that keep them safe. No Spark here: the job builds the SQL with these functions and runs it.

Every write to an Iceberg table adds files and never changes one, so without maintenance a table only grows:
- rewrite_data_files compacts small files into fewer, larger ones (and moves old files to the current partition spec);
- rewrite_manifests does the same for the manifests that list them;
- expire_snapshots drops old snapshots and deletes the files only they used: compaction alone frees nothing;
- remove_orphan_files deletes files no snapshot references, e.g. those left by a commit that lost a race.
"""

import datetime as dt
from dataclasses import dataclass

# Iceberg itself refuses to remove orphans younger than a day: a file that looks orphaned may belong to a commit that
# is still running. Our default leaves more margin.
MIN_ORPHAN_AGE_DAYS = 1


@dataclass(frozen=True)
class Options:
    expire_older_than_days: int = 7
    retain_last: int = 5  # snapshots kept whatever their age: the minimum history for time travel and rollback
    orphan_older_than_days: int = 3
    rewrite_all: bool = False  # rewrite every file, e.g. once after a partition change; default: only small files
    dry_run: bool = False  # orphans only: list them without deleting

    def __post_init__(self):
        problems = []
        if self.retain_last < 1:
            problems.append("retain_last must be at least 1: a table needs its current snapshot")
        if self.expire_older_than_days < 0:
            problems.append("expire_older_than_days can't be negative")
        if self.orphan_older_than_days < MIN_ORPHAN_AGE_DAYS:
            problems.append(f"orphan_older_than_days must be at least {MIN_ORPHAN_AGE_DAYS}: younger files may belong to a commit in progress")
        if problems:
            raise ValueError("; ".join(problems))


def _timestamp(moment: dt.datetime) -> str:
    """A SQL timestamp literal in UTC: the procedures take a literal, not an expression like current_timestamp()."""
    return f"TIMESTAMP '{moment.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}'"


def rewrite_data_files_sql(catalog: str, table: str, sort_order: str | None, rewrite_all: bool) -> str:
    """`sort` for a table with a sort order, `binpack` otherwise. binpack on a sorted table range-distributes the
    rewrite over spark.sql.shuffle.partitions and wrote 200 files out of 10 (measured); sort wrote 1."""
    strategy = "sort" if sort_order else "binpack"
    options = "map('rewrite-all', 'true')" if rewrite_all else "map('min-input-files', '2')"
    return f"CALL `{catalog}`.system.rewrite_data_files(table => '{table}', strategy => '{strategy}', options => {options})"


def rewrite_manifests_sql(catalog: str, table: str) -> str:
    return f"CALL `{catalog}`.system.rewrite_manifests(table => '{table}')"


def expire_cutoff(options: Options, now: dt.datetime, stream_watermark: dt.datetime | None) -> dt.datetime:
    """Snapshots older than this may expire. For a table a stream reads (a watermark in DynamoDB), never past the last
    snapshot the stream consumed: its checkpoint needs that snapshot to find what came after (docs/PLAN.md step 9)."""
    cutoff = now - dt.timedelta(days=options.expire_older_than_days)
    return cutoff if stream_watermark is None else min(cutoff, stream_watermark)


def expire_snapshots_sql(catalog: str, table: str, options: Options, older_than: dt.datetime) -> str:
    return (
        f"CALL `{catalog}`.system.expire_snapshots(table => '{table}', older_than => {_timestamp(older_than)}, "
        f"retain_last => {options.retain_last})"
    )


def remove_orphan_files_sql(catalog: str, table: str, options: Options, now: dt.datetime) -> str:
    older_than = now - dt.timedelta(days=options.orphan_older_than_days)
    return (
        f"CALL `{catalog}`.system.remove_orphan_files(table => '{table}', older_than => {_timestamp(older_than)}, "
        f"dry_run => {'true' if options.dry_run else 'false'})"
    )


def procedure_table(database: str, table: str) -> str:
    """The table argument of a procedure: `db`.table, quoted because our databases start with a digit."""
    return f"`{database}`.{table}"
