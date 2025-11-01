"""
Simple CSV -> DB loader for the repo sample.

This script demonstrates how to:
- load `sales_data.csv` with pandas (parse dates, handle NaN)
- convert columns to appropriate dtypes
- write the DataFrame to a local SQLite database (and a template for other DBs via SQLAlchemy)

Usage examples:
    python load_sales.py --to sqlite --db-path sales.db --if-exists replace

For a remote DB (Postgres/MySQL), use:
    python load_sales.py --to sqlalchemy --conn "postgresql+psycopg2://user:pass@host:port/dbname" --if-exists append

The script is defensive: it will try to use SQLAlchemy when available and fall back to sqlite3 for local SQLite.
"""
from __future__ import annotations
import argparse
import os
import sys
from typing import Optional

import pandas as pd8


def load_csv(path: str = "sales_data.csv") -> pd.DataFrame:
    """Load CSV into a pandas DataFrame with sensible defaults for this dataset.

    - parses `order_date` as datetime
    - coerces `amount` to numeric (NaN on bad values)
    - preserves columns seen in the sample CSV
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(
        path,
        parse_dates=["order_date"],
        dtype={"order_id": "Int64", "customer_name": "string", "state": "string"},
        na_values=["", "NaN"],
    )
    # Ensure numeric amount
    df["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
    return df


def load_to_sqlite(df: pd.DataFrame, db_path: str = "sales.db", table_name: str = "sales", if_exists: str = "replace") -> None:
    """Write DataFrame to a local SQLite database.

    Tries to use SQLAlchemy if installed; otherwise falls back to sqlite3.
    """
    print(f"Writing {len(df)} rows to SQLite db '{db_path}' table '{table_name}' (if_exists={if_exists})")
    try:
        from sqlalchemy import create_engine

        engine = create_engine(f"sqlite:///{db_path}")
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        engine.dispose()
        print("Write to SQLite via SQLAlchemy successful.")
    except Exception as e:
        # Fallback: use sqlite3 connection (pandas supports sqlite3.Connection)
        print("SQLAlchemy not available or failed, falling back to sqlite3. Error:", e)
        import sqlite3

        conn = sqlite3.connect(db_path)
        try:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print("Write to SQLite via sqlite3 successful.")
        finally:
            conn.close()


def load_to_db_sqlalchemy(df: pd.DataFrame, connection_string: str, table_name: str = "sales", if_exists: str = "append") -> None:
    """Write DataFrame to any DB supported by SQLAlchemy using a connection string.

    Example connection strings:
      - Postgres: postgresql+psycopg2://user:pass@host:port/dbname
      - MySQL: mysql+pymysql://user:pass@host:port/dbname
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        raise RuntimeError("SQLAlchemy is required for remote DB loading. Install it with `pip install sqlalchemy`. ")

    engine = create_engine(connection_string)
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"Write to DB via SQLAlchemy successful using connection: {connection_string}")
    finally:
        engine.dispose()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Load sales_data.csv into a database using pandas.")
    parser.add_argument("--csv", default="sales_data.csv", help="Path to input CSV")
    parser.add_argument("--to", choices=("sqlite", "sqlalchemy"), default="sqlite", help="Destination type")
    parser.add_argument("--db-path", default="sales.db", help="SQLite db path (when --to sqlite)")
    parser.add_argument("--conn", default=None, help="SQLAlchemy connection string (when --to sqlalchemy)")
    parser.add_argument("--table", default="sales", help="Table name to write into")
    parser.add_argument("--if-exists", choices=("fail", "replace", "append"), default="replace", help="Behavior if table exists")

    args = parser.parse_args(argv)

    try:
        df = load_csv(args.csv)
    except Exception as exc:
        print("Failed to load CSV:", exc)
        return 2

    print("Loaded CSV: rows=", len(df))
    print(df.head(3).to_string(index=False))

    if args.to == "sqlite":
        load_to_sqlite(df, db_path=args.db_path, table_name=args.table, if_exists=args.if_exists)
    else:
        if not args.conn:
            print("--conn is required when --to sqlalchemy")
            return 3
        load_to_db_sqlalchemy(df, args.conn, table_name=args.table, if_exists=args.if_exists)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
