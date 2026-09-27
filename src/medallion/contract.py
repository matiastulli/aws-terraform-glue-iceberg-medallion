"""Write contract: what a job writes must match the table's DDL exactly, column for column.

Jobs never create or change tables, so before every write they compare the DataFrame with the table and fail on any
difference, instead of relying on what the table format happens to reject (see docs/PLAN.md step 2 for what Iceberg
accepts on its own).
"""


def schema_mismatches(written: list[tuple[str, str]], table: list[tuple[str, str]]) -> list[str]:
    """Compares (column, type) pairs, as given by DataFrame.dtypes, and describes every difference."""
    written_types, table_types = dict(written), dict(table)
    problems = [f"missing column {name} ({table_types[name]})" for name in table_types if name not in written_types]
    problems += [f"extra column {name} ({written_types[name]})" for name in written_types if name not in table_types]
    problems += [
        f"column {name} is {written_types[name]} but the table has {table_types[name]}"
        for name in table_types
        if name in written_types and written_types[name] != table_types[name]
    ]
    return problems


def raise_if_schema_mismatch(written: list[tuple[str, str]], table: list[tuple[str, str]], table_name: str) -> None:
    if problems := schema_mismatches(written, table):
        raise ValueError(
            f"what this job writes doesn't match the DDL of {table_name}: " + "; ".join(problems)
            + ". Change the code, or change the table with a new migration."
        )
