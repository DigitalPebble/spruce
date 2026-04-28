import argparse
import argparse
import duckdb
import os
from pathlib import Path

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

FILE_EXTENSION = "parquet"          # "parquet" or "csv"

# Exact column names to drop
SENSITIVE_COLUMNS_EXACT = [
    "resource_tags",
    "savings_plan_savings_plan_a_r_n",
    "reservation_reservation_a_r_n",
]

# Prefix patterns — any column starting with these will be dropped
SENSITIVE_PREFIXES = [
    "bill_", 
    "line_item_usage_account_",
]

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def get_sensitive_columns(con, table_name: str) -> list[str]:
    """Return list of columns to drop based on exact names and prefix patterns."""
    all_columns = [
        row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()
    ]

    to_drop = set()

    for col in all_columns:
        if col in SENSITIVE_COLUMNS_EXACT:
            to_drop.add(col)
        elif any(col.startswith(prefix) for prefix in SENSITIVE_PREFIXES):
            to_drop.add(col)

    return list(to_drop)


def sanitize_file(con, filepath: str, output_dir: str, ext: str):
    """Load a CUR file, drop sensitive columns, write cleaned output."""
    filename = Path(filepath).name
    output_file = os.path.join(output_dir, filename)

    print(f"Processing: {filename}")

    # Load file into a temp view
    if ext == "parquet":
        con.execute(f"CREATE OR REPLACE VIEW cur_raw AS SELECT * FROM read_parquet('{filepath}')")
    else:
        con.execute(f"CREATE OR REPLACE VIEW cur_raw AS SELECT * FROM read_csv_auto('{filepath}')")

    # Identify sensitive columns present in this file
    sensitive = get_sensitive_columns(con, "cur_raw")

    if sensitive:
        print(f"  Dropping {len(sensitive)} sensitive columns: {sensitive}")
    else:
        print("  No sensitive columns found — writing file as-is")

    # Build SELECT excluding sensitive columns
    all_cols = [row[0] for row in con.execute("DESCRIBE cur_raw").fetchall()]
    keep_cols = [f'"{c}"' for c in all_cols if c not in sensitive]
    select_sql = f"SELECT {', '.join(keep_cols)} FROM cur_raw"

    # Write output
    if ext == "parquet":
        con.execute(f"COPY ({select_sql}) TO '{output_file}' (FORMAT PARQUET)")
    else:
        con.execute(f"COPY ({select_sql}) TO '{output_file}' (FORMAT CSV, HEADER)")

    print(f"  Saved to: {output_file}")


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Remove sensitive columns from CUR report files.")
    parser.add_argument("input",  help="Path to folder containing raw CUR files")
    parser.add_argument("output", help="Path to folder where sanitized files will be written")
    parser.add_argument("--ext", default=FILE_EXTENSION, choices=["parquet", "csv"],
                        help="File extension to process (default: parquet)")
    return parser.parse_args()


def main():
    args = parse_args()
    input_path  = args.input
    output_path = args.output
    ext = args.ext

    os.makedirs(output_path, exist_ok=True)

    # Gather input files — rglob picks up files in subfolders too
    input_files = list(Path(input_path).rglob(f"*.{ext}"))
    if not input_files:
        print(f"No .{ext} files found in {input_path}")
        return

    # Detect whether any subfolders are present
    has_subfolders = any(f.parent != Path(input_path) for f in input_files)
    if has_subfolders:
        print("Subfolders detected — output structure will mirror input\n")

    print(f"Found {len(input_files)} file(s) to process\n")

    con = duckdb.connect()

    for filepath in input_files:
        # Preserve relative subfolder path when writing output
        relative = filepath.relative_to(input_path)
        output_file_dir = Path(output_path) / relative.parent
        os.makedirs(output_file_dir, exist_ok=True)
        sanitize_file(con, str(filepath), str(output_file_dir), ext)

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
