#!/usr/bin/env python3
"""
Emit an Athena CREATE EXTERNAL TABLE for SPRUCE-enriched output.

Athena has no read_parquet(): CREATE EXTERNAL TABLE always needs an explicit
column list, because the Glue Data Catalog holds the schema and the SerDe maps
Parquet fields onto it by name. An AWS CUR carries around 150 columns, so the
list is read from the output itself rather than written by hand.

Reading the schema also keeps the DDL right across pipelines. An AWS CUR, an
AWS FOCUS export, an Azure cost details export and an Azure FOCUS export
produce four different column sets, and a DDL fixed for one of them is wrong
for the other three.

The table uses partition projection rather than registered partitions, so
Athena derives the BILLING_PERIOD list arithmetically instead of listing S3.
Nothing has to be crawled or repaired after an enrichment run: a new month is
queryable as soon as it lands.

Run with:
    python3 reporting/athena/generate_ddl.py -i output/ -l s3://bucket/prefix/
"""

from __future__ import annotations

import argparse
import re
import sys

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required - run: pip install -r reporting/requirements-report.txt")

# The hive partition column SparkJob writes. Declared in PARTITIONED BY, and
# never in the column list, which Athena rejects as a duplicate.
PARTITION = "BILLING_PERIOD"

# DuckDB's type names to Hive's. Anything absent raises rather than guessing,
# since a wrong type reads back as NULL instead of failing loudly.
SCALARS = {
    "VARCHAR": "string",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BIGINT": "bigint",
    "INTEGER": "int",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "BLOB": "binary",
}


def split_top_level(s: str) -> list[str]:
    """Split on commas outside any bracket, for MAP and STRUCT type bodies."""
    parts: list[str] = []
    depth = 0
    current = ""
    for ch in s:
        if ch in "(<[":
            depth += 1
        elif ch in ")>]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch
    parts.append(current)
    return [p.strip() for p in parts]


def hive_type(duckdb_type: str) -> str:
    t = duckdb_type.strip()
    if t.upper() in SCALARS:
        return SCALARS[t.upper()]
    if t.upper().startswith("DECIMAL"):
        return t.lower()
    match = re.match(r"^MAP\((.*)\)$", t, re.IGNORECASE)
    if match:
        key, value = split_top_level(match.group(1))
        return f"map<{hive_type(key)},{hive_type(value)}>"
    if t.endswith("[]"):
        return f"array<{hive_type(t[:-2])}>"
    match = re.match(r"^STRUCT\((.*)\)$", t, re.IGNORECASE)
    if match:
        fields = []
        for field in split_top_level(match.group(1)):
            name, _, field_type = field.strip().partition(" ")
            fields.append(f'{name.strip(chr(34))}:{hive_type(field_type)}')
        return f"struct<{','.join(fields)}>"
    raise SystemExit(f"No Hive equivalent for DuckDB type '{duckdb_type}'")


def columns(con, parquet_glob: str) -> list[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)"
    ).fetchall()
    out = []
    for name, dtype, *_ in rows:
        if str(name).upper() == PARTITION:
            continue
        out.append(f"  `{name}` {hive_type(str(dtype))}")
    if not out:
        sys.exit(f"No columns found in '{parquet_glob}'")
    return out


def ddl(column_lines: list[str], table: str, location: str, first_period: str) -> str:
    key = PARTITION.lower()
    joined = ",\n".join(column_lines)
    return f"""-- Athena external table over SPRUCE-enriched output.
-- {len(column_lines)} columns, partitioned by {PARTITION}.
--
-- Partition projection means Athena derives the partition list from the range
-- below rather than listing S3, so no crawler is needed and no MSCK REPAIR is
-- needed after an enrichment run. Widen the range as the data grows; 'NOW'
-- keeps the upper end open.

CREATE EXTERNAL TABLE IF NOT EXISTS {table} (
{joined}
)
PARTITIONED BY (`{PARTITION}` string)
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.{key}.type' = 'date',
  'projection.{key}.format' = 'yyyy-MM',
  'projection.{key}.range' = '{first_period},NOW',
  'projection.{key}.interval' = '1',
  'projection.{key}.interval.unit' = 'MONTHS',
  -- The literal S3 path, which is case sensitive, with the projected value
  -- substituted. Glue lower-cases the column name, hence ${{{key}}} against an
  -- upper-case directory prefix.
  'storage.location.template' = '{location}{PARTITION}=${{{key}}}/'
);
"""


def main():
    parser = argparse.ArgumentParser(
        description="Generate the Athena DDL for SPRUCE-enriched output"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Path to enriched Parquet directory, glob, or S3 URI. Only "
                             "the schema is read, so a single file or one partition is "
                             "enough.")
    parser.add_argument("-l", "--location", required=True,
                        help="S3 prefix the table sits on, e.g. s3://bucket/enriched/. "
                             "This is where the BILLING_PERIOD= directories live.")
    parser.add_argument("-t", "--table", default="spruce.enriched",
                        help="Qualified table name to create (default: spruce.enriched). "
                             "The database must exist already.")
    parser.add_argument("-f", "--first-period", default="2024-01", metavar="YYYY-MM",
                        help="Earliest billing period the projection range covers "
                             "(default: 2024-01). Periods before it are invisible to "
                             "the table, so err early.")
    parser.add_argument("-o", "--output", default=None,
                        help="File to write the DDL to; defaults to stdout.")
    args = parser.parse_args()

    inp = args.input.rstrip("/")
    parquet_glob = inp if inp.endswith(".parquet") else f"{inp}/**/*.parquet"
    location = args.location if args.location.endswith("/") else args.location + "/"

    con = duckdb.connect(":memory:")
    if args.input.startswith("s3"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
    try:
        column_lines = columns(con, parquet_glob)
    except SystemExit:
        raise
    except Exception as exc:
        sys.exit(f"Failed to read the schema from '{parquet_glob}': {exc}")

    statement = ddl(column_lines, args.table, location, args.first_period)
    if args.output:
        with open(args.output, "w") as handle:
            handle.write(statement)
        sys.stderr.write(f"{len(column_lines)} columns -> {args.output}\n")
    else:
        print(statement, end="")


if __name__ == "__main__":
    main()
