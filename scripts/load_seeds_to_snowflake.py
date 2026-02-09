#!/usr/bin/env python3
"""
Load CSV seed files into Snowflake RAW schema.
This is an alternative to `dbt seed` when you want to load data
directly via the Snowflake connector (e.g., for large datasets).

Usage:
    pip install snowflake-connector-python pandas
    export SNOWFLAKE_ACCOUNT='...'
    export SNOWFLAKE_USER='...'
    export SNOWFLAKE_PASSWORD='...'
    python scripts/load_seeds_to_snowflake.py
"""

import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORMING"),
        schema="RAW",
    )


def load_csv(conn, filepath: str):
    table_name = os.path.splitext(os.path.basename(filepath))[0].upper()
    print(f"Loading {filepath} -> RAW.{table_name}")

    df = pd.read_csv(filepath)
    df.columns = [col.upper() for col in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name, auto_create_table=True, overwrite=True
    )

    if success:
        print(f"  -> Loaded {nrows} rows into {table_name}")
    else:
        print(f"  -> FAILED to load {table_name}")


def main():
    seed_dir = os.path.join(os.path.dirname(__file__), "..", "seeds")
    csv_files = sorted(glob.glob(os.path.join(seed_dir, "raw_*.csv")))

    if not csv_files:
        print("No CSV files found in seeds/ directory")
        return

    print(f"Found {len(csv_files)} CSV files to load")
    conn = get_connection()

    try:
        for filepath in csv_files:
            load_csv(conn, filepath)
    finally:
        conn.close()

    print("\nDone! All seed data loaded into Snowflake RAW schema.")


if __name__ == "__main__":
    main()
