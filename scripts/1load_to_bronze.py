#!/usr/bin/env python3
"""
load_to_duckdb.py

Ingests all generated datasets into DuckDB bronze tables.
- Reads from: scripts/data_raw/
- Writes to: duckdb/warehouse.duckdb
"""

import duckdb
import os
import pandas as pd 
from pathlib import Path
from tqdm import tqdm  # ✅ Added for progress bar
import time  # optional for realistic progress display

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "scripts" / "data_raw"
DB_PATH = BASE_DIR / "duckdb" / "warehouse.duckdb"

# Ensure DB folder exists
os.makedirs(DB_PATH.parent, exist_ok=True)

# Connect to DuckDB
con = duckdb.connect(str(DB_PATH))
con.execute("PRAGMA threads=4;")
con.execute("CREATE SCHEMA IF NOT EXISTS main;")


print(f"📦 Loading datasets from: {RAW_DIR}")
print(f"📚 Target database: {DB_PATH}\n")

# -------------------------------------------------------------------
# Helper to load CSV / Parquet / JSONL / XLSX dynamically
# -------------------------------------------------------------------
def load_csv(table, path_pattern):
    con.execute(f"DROP TABLE IF EXISTS {table};")
    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_csv_auto('{path_pattern}', ignore_errors=true);
    """)
    print(f"✅ {table} loaded (CSV)")


def load_parquet(table, path_pattern):
    con.execute(f"DROP TABLE IF EXISTS {table};")
    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_parquet('{path_pattern}');
    """)
    print(f"✅ {table} loaded (Parquet)")


def load_jsonl(table, path_pattern):
    temp_table = f"{table}_tmp"
    con.execute(f"DROP TABLE IF EXISTS {temp_table};")
    con.execute(f"""
        CREATE TABLE {temp_table} AS
        SELECT * FROM read_json_auto('{path_pattern}', ignore_errors=true);
    """)
    valid_count = con.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
    con.execute(f"DROP TABLE IF EXISTS {table};")
    con.execute(f"ALTER TABLE {temp_table} RENAME TO {table};")
    print(f"✅ {table} loaded (JSONL, {valid_count:,} valid rows)")

def load_xlsx(table, path_pattern):
    
    # Read Excel with Pandas
    df = pd.read_excel(path_pattern)
    temp_name = f"{table}_temp"

    # Register DataFrame as a temporary DuckDB view
    con.register(temp_name, df)

    # Check if table/view exists and drop appropriately
    existing = con.execute(f"""
        SELECT table_type 
        FROM information_schema.tables 
        WHERE table_name = '{table}' 
        AND table_schema = 'main';
    """).fetchone()

    if existing:
        if existing[0] == "VIEW":
            con.execute(f"DROP VIEW {table};")
        else:
            con.execute(f"DROP TABLE {table};")

    # Create the new table from the registered DataFrame
    con.execute(f"CREATE TABLE {table} AS SELECT * FROM {temp_name};")
    print(f"✅ {table} loaded (Excel via Pandas)")


# -------------------------------------------------------------------
# Bronze Tables with progress bar
# -------------------------------------------------------------------
tasks = [
    ("bronze_customers", load_csv, RAW_DIR / "customers.csv"),
    ("bronze_products", load_csv, RAW_DIR / "products.csv"),
    ("bronze_stores", load_csv, RAW_DIR / "stores.csv"),
    ("bronze_suppliers", load_csv, RAW_DIR / "suppliers.csv"),
    ("bronze_shipments", load_parquet, RAW_DIR / "shipments.parquet"),
    ("bronze_returns", load_parquet, RAW_DIR / "returns_v1.parquet"),
    ("bronze_orders_header", load_csv, RAW_DIR / "orders" / "*" / "*.csv"),
    ("bronze_orders_lines", load_csv, RAW_DIR / "orders" / "*" / "*.csv"),
    ("bronze_events", load_jsonl, RAW_DIR / "events" / "*.jsonl"),
    ("bronze_sensors", load_csv, RAW_DIR / "sensors" / "*" / "*" / "*.csv"),
    ("bronze_exchange_rates", load_xlsx, RAW_DIR / "exchange_rates.xlsx"),
]

# ✅ Progress bar + ETA
for table, func, path in tqdm(tasks, desc="🚀 Loading bronze tables", unit="table"):
    func(table, path)
    time.sleep(0.1)  # optional — for smoother visual ETA updates

# -------------------------------------------------------------------
# Validation Summary (fixed)
# -------------------------------------------------------------------
print("\n📊 DuckDB Table Summary:\n")

tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name;
""").fetchall()

summary_data = []
for schema, table in tables:
    # Count rows and columns dynamically
    row_count = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";').fetchone()[0]
    col_count = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}';
    """).fetchone()[0]
    summary_data.append((schema, table, row_count, col_count))

import pandas as pd
summary = pd.DataFrame(summary_data, columns=["schema", "table_name", "row_count", "column_count"])
print(summary.to_string(index=False))
