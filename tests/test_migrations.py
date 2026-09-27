import pytest

from medallion.migrations import load_migrations, moved_migrations, parse_migrations, pending_migrations, render, split_statements


def files(*paths):
    return {path: f"-- {path}\nSELECT 1;" for path in paths}


def run_order(*paths):
    return [m.path for m in parse_migrations(files(*paths))]


def applied_from(migrations):
    return {m.checksum: (m.key, m.version, m.path) for m in migrations}


def test_the_committed_migrations_are_valid():
    # Runs in CI, so a misplaced, badly named, duplicated or missing migration fails before anyone applies it.
    assert [m.key for m in load_migrations()][0] == "bronze_open_meteo_hourly"


def test_run_order_is_layer_folders_then_table_then_version():
    assert run_order(
        "ops/ddl/watermarks/v001_create.sql",
        "02_gold/ddl/agg_readings_daily/v001_create.sql",
        "01_silver/ddl/readings/v010_alter.sql",
        "01_silver/ddl/readings/v001_create.sql",
        *[f"01_silver/ddl/readings/v{v:03d}_alter.sql" for v in range(2, 10)],
        "00_bronze/ddl/open_meteo_hourly/v001_create.sql",
    ) == [
        "00_bronze/ddl/open_meteo_hourly/v001_create.sql",
        "01_silver/ddl/readings/v001_create.sql",
        *[f"01_silver/ddl/readings/v{v:03d}_alter.sql" for v in range(2, 11)],  # v010 after v009, not after v001
        "02_gold/ddl/agg_readings_daily/v001_create.sql",
        "ops/ddl/watermarks/v001_create.sql",  # ops runs after the layers it supports
    ]


@pytest.mark.parametrize(
    "paths, message",
    [
        (["01_silver/ddl/readings_v001_create.sql"], "must be src/<NN_layer or ops>/ddl/<table>/<file>"),
        (["silver/ddl/readings/v001_create.sql"], "must be src/<NN_layer or ops>/ddl/<table>/<file>"),
        (["01_silver/ddl/Readings/v001_create.sql"], "must be the table name in lowercase"),
        (["01_silver/ddl/readings/create_v001.sql"], "must be named v<NNN>_"),
        (["01_silver/ddl/readings/v001_update.sql"], "must be named v<NNN>_"),
        (["01_silver/ddl/readings/v001_alter.sql"], "v001 must create the table"),
        (["01_silver/ddl/readings/v001_create.sql", "01_silver/ddl/readings/v002_create.sql"], "create can only be v001"),
        (["01_silver/ddl/readings/v001_create.sql", "01_silver/ddl/readings/v003_alter.sql"], r"missing versions \['v002'\]"),
    ],
)
def test_misplaced_misnamed_or_inconsistent_migrations_are_rejected(paths, message):
    with pytest.raises(ValueError, match=message):
        parse_migrations(files(*paths))


def test_only_migrations_not_yet_applied_are_pending():
    migrations = parse_migrations(files("00_bronze/ddl/open_meteo_hourly/v001_create.sql", "01_silver/ddl/readings/v001_create.sql", "01_silver/ddl/readings/v002_alter.sql"))

    assert [m.path for m in pending_migrations(migrations, applied_from(migrations[:2]))] == ["01_silver/ddl/readings/v002_alter.sql"]
    assert pending_migrations(migrations, applied_from(migrations)) == []


def test_a_file_that_moved_is_still_the_same_migration():
    before = parse_migrations(files("01_silver/ddl/observations/v001_create.sql"))
    applied = applied_from(before)
    after = parse_migrations({"01_silver/ddl/readings/v001_create.sql": before[0].sql})

    assert pending_migrations(after, applied) == []
    assert [m.path for m in moved_migrations(after, applied)] == ["01_silver/ddl/readings/v001_create.sql"]


def test_editing_an_applied_migration_is_refused():
    original = parse_migrations(files("01_silver/ddl/readings/v001_create.sql"))
    edited = parse_migrations({"01_silver/ddl/readings/v001_create.sql": "CREATE TABLE x (id INT);"})

    with pytest.raises(ValueError, match="changed after it was applied"):
        pending_migrations(edited, applied_from(original))


def test_an_applied_migration_whose_file_is_gone_is_refused():
    migrations = parse_migrations(files("01_silver/ddl/readings/v001_create.sql"))
    applied = applied_from(migrations) | {"deadbeef": ("silver_readings", 2, "01_silver/ddl/readings/v002_alter.sql")}

    with pytest.raises(ValueError, match="was applied but is gone"):
        pending_migrations(migrations, applied)


def test_semicolons_inside_quotes_and_comments_do_not_split_statements():
    sql = """
    -- a comment; with a semicolon
    CREATE TABLE `a;b`.t (x STRING) COMMENT 'raw; untouched';
    ALTER TABLE t SET TBLPROPERTIES ('k' = 'v;w');
    """

    assert split_statements(sql) == [
        "CREATE TABLE `a;b`.t (x STRING) COMMENT 'raw; untouched'",
        "ALTER TABLE t SET TBLPROPERTIES ('k' = 'v;w')",
    ]


def test_placeholders_are_replaced_and_a_missing_value_is_refused():
    assert render("`${catalog}`.`${gold_db}`.t", {"catalog": "glue_catalog", "gold_db": "02_gold"}) == "`glue_catalog`.`02_gold`.t"

    with pytest.raises(ValueError, match=r"no value for placeholders \['silver_db'\]"):
        render("`${catalog}`.`${silver_db}`.t", {"catalog": "glue_catalog"})
