"""Table DDL as write-once SQL migrations, kept next to the code of their entity, applied by the apply_ddl Glue job.

Code is grouped by entity: one folder per entity inside each layer, holding its tables' migrations and the job that
writes them. A migration is named after its table and version, so one folder can hold an entity's main table and its
quarantine:

    src/00_bronze/open_meteo_hourly/ddl_open_meteo_hourly_v001_create.sql
    src/01_silver/readings/ddl_readings_v001_create.sql
                          /ddl_readings_v002_alter.sql
                          /ddl_readings_quarantine_v001_create.sql
                          /glue_job_clean_readings.py

A table's migrations live in its entity folder: the table is the entity (`readings`) or starts with it
(`readings_quarantine`). Terraform owns the Glue databases, so no migration creates them.

**A migration is identified by the SHA-256 of its content, not by its path.** Reorganising folders or renaming a
table moves files, and that must not look like a different migration. Editing or deleting an applied migration fails.

The runner (src/ops/schema_migrations/glue_job_apply_ddl.py) only executes SQL and records history. Everything that
decides *what* runs and in which order lives here, free of Spark, so it can be unit-tested.
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

# src/medallion/migrations.py -> src/
SRC_DIR = Path(__file__).resolve().parents[1]

# A layer folder (00_bronze …) or `ops`, which holds tooling tables and runs last, after the layers it supports.
LAYER_FOLDER = re.compile(r"^(?:(?P<order>\d{2})_(?P<layer>bronze|silver|gold)|(?P<ops>ops))$")
ENTITY_FOLDER = re.compile(r"^[a-z][a-z0-9_]*$")
FILE_NAME = re.compile(r"^ddl_(?P<table>[a-z][a-z0-9_]*?)_v(?P<version>\d{3})_(?P<verb>create|alter|rename|drop)\.sql$")
GLOB = "*/*/ddl_*.sql"
PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")
# The Spark catalog name and the Glue databases: what differs between the real lakehouse and a throwaway copy.
PLACEHOLDERS = ("catalog", "bronze_db", "silver_db", "gold_db", "ops_db")

OPS_ORDER = 99
HISTORY_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    key: str  # what the migration versions: "<layer>_<table>", e.g. "bronze_open_meteo_hourly"
    version: int
    verb: str
    path: str  # relative to src/, e.g. "01_silver/readings/ddl_readings_v001_create.sql"
    sql: str
    folder_order: int

    @property
    def checksum(self) -> str:
        """SHA-256 of the file as written: the migration's identity, so moving or renaming the file is free."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def parse_migrations(files: dict[str, str]) -> list[Migration]:
    """Validates migration files (path relative to src/ -> content) and returns them in the order they must run.

    Raises ValueError listing every problem found. Order: folders (00, 01, 02, then ops), tables by name, and each
    table's versions ascending.
    """
    problems, migrations = [], []
    for path, sql in sorted(files.items()):
        parts = Path(path).parts
        folder = LAYER_FOLDER.match(parts[0]) if len(parts) == 3 else None
        if not folder:
            problems.append(f"{path}: must be src/<NN_layer or ops>/<entity>/<file>, e.g. 01_silver/readings/ddl_readings_v001_create.sql")
            continue
        entity = parts[1]
        if not ENTITY_FOLDER.match(entity):
            problems.append(f"{path}: the folder {entity!r} must be an entity name in lowercase letters, digits and underscores")
            continue
        name = FILE_NAME.match(parts[2])
        if not name:
            problems.append(f"{path}: must be named ddl_<table>_v<NNN>_<create|alter|rename|drop>.sql")
            continue
        table = name["table"]
        if table != entity and not table.startswith(f"{entity}_"):
            problems.append(f"{path}: table {table!r} belongs in its own entity folder: the table must be {entity!r} or start with '{entity}_'")
            continue

        version, verb = int(name["version"]), name["verb"]
        # A table's history starts by creating it; everything after that changes what is already there.
        if verb == "create" and version != 1:
            problems.append(f"{path}: create can only be v001; change an existing table with alter, rename or drop")
        if verb != "create" and version == 1:
            problems.append(f"{path}: v001 must create the table")
        layer = folder["layer"] or folder["ops"]
        order = int(folder["order"]) if folder["order"] else OPS_ORDER
        migrations.append(Migration(f"{layer}_{table}", version, verb, path, sql, order))

    by_key: dict[str, list[Migration]] = {}
    for migration in migrations:
        by_key.setdefault(migration.key, []).append(migration)
    for key, versions in sorted(by_key.items()):
        numbers = [m.version for m in versions]
        if duplicates := sorted({n for n in numbers if numbers.count(n) > 1}):
            problems.append(f"{key}: versions {['v%03d' % n for n in duplicates]} are used by more than one file")
        if missing := sorted(set(range(1, max(numbers) + 1)) - set(numbers)):
            problems.append(f"{key}: missing versions {['v%03d' % n for n in missing]}")

    if problems:
        raise ValueError("invalid migrations:\n  " + "\n  ".join(problems))
    return sorted(migrations, key=lambda m: (m.folder_order, m.key, m.version))


def load_migrations(src_dir: Path = SRC_DIR) -> list[Migration]:
    """The migrations in a source tree (tests and CI). The Glue job reads the same files from S3 instead."""
    return parse_migrations({path.relative_to(src_dir).as_posix(): path.read_text(encoding="utf-8") for path in sorted(src_dir.glob(GLOB))})


def pending_migrations(migrations: list[Migration], applied: dict[str, tuple[str, int, str]]) -> list[Migration]:
    """Returns the migrations not applied yet, in run order.

    `applied` maps checksum -> (key, version, path) from the history table. A file that moved keeps its checksum, so it
    is recognised wherever it now lives. What still fails is an applied migration that was **edited** (its key and
    version are still there, with different content) or **deleted** (neither its content nor its key and version).
    """
    by_checksum = {m.checksum: m for m in migrations}
    by_id = {(m.key, m.version): m for m in migrations}
    problems = []
    for checksum, (key, version, path) in sorted(applied.items(), key=lambda item: item[1]):
        if checksum in by_checksum:
            continue
        if (key, version) in by_id:
            problems.append(f"{by_id[(key, version)].path} changed after it was applied; write a new version instead of editing it")
        else:
            problems.append(f"{path} was applied but is gone: no file has its content, and {key} v{version:03d} no longer exists")
    if problems:
        raise ValueError("migration history doesn't match the migration files:\n  " + "\n  ".join(problems))
    return [m for m in migrations if m.checksum not in applied]


def moved_migrations(migrations: list[Migration], applied: dict[str, tuple[str, int, str]]) -> list[Migration]:
    """Applied migrations whose file has moved or been renumbered, so the history can be refreshed."""
    return [m for m in migrations if m.checksum in applied and applied[m.checksum] != (m.key, m.version, m.path)]


def render(sql: str, values: dict[str, str]) -> str:
    """Replaces ${name} placeholders. An unknown or missing placeholder raises instead of reaching the catalog."""
    names = set(PLACEHOLDER.findall(sql))
    if unknown := names - values.keys():
        raise ValueError(f"no value for placeholders {sorted(unknown)}")
    return PLACEHOLDER.sub(lambda match: values[match[1]], sql)


def split_statements(sql: str) -> list[str]:
    """Splits a migration into statements on `;`, ignoring semicolons inside quotes, backticks and `--` comments."""
    statements, current, quote, index = [], [], None, 0
    while index < len(sql):
        char = sql[index]
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in ("'", '"', "`"):
            quote = char
            current.append(char)
        elif sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end == -1 else end
            continue
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    statements.append("".join(current))
    return [statement.strip() for statement in statements if statement.strip()]
