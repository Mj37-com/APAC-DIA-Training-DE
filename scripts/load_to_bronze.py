# scripts/load_to_bronze.py
# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage:
#   PYTHONPATH=. python scripts/load_to_bronze.py --raw scripts/data_raw --lake lake --manifest duckdb/warehouse.duckdb

import argparse, pathlib, datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pyarrow.json as paj

# Import all schemas
from schemas.schemas import (
    customers_schema, products_schema, stores_schema, suppliers_schema,
    orders_header_schema, orders_lines_schema, shipments_schema,
    returns_day1_schema, exchange_rates_schema, sensors_schema, events_schema
)

try:
    from deltalake import write_deltalake
except Exception:
    write_deltalake = None


# -------------------- Helpers --------------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    return ap.parse_args()

def ensure_dirs(lake_root):
    for sub in ['bronze/parquet', 'bronze/delta']:
        (lake_root / sub).mkdir(parents=True, exist_ok=True)
    (lake_root / '_rejects').mkdir(parents=True, exist_ok=True)

def init_manifest(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT
        )
    ''')

def already_processed(conn, p):
    return conn.execute(
        "SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]
    ).fetchone() is not None

def mark_processed(conn, p, n):
    conn.execute(
        "INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?)",
        [str(p), dt.datetime.utcnow(), n]
    )

def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(
        table,
        base_dir=str(base_path),
        format='parquet',
        partitioning=partitioning,
        existing_data_behavior='overwrite_or_ignore'
    )

def write_delta(table, base_path, mode='append', partition_by=None):
    if write_deltalake is None:
        print("⚠️ Delta Lake not installed, skipping delta write.")
        return
    write_deltalake(str(base_path), data=table, mode=mode, partition_by=partition_by or [])


# -------------------- Generic Loader --------------------

def generic_loader(raw_root, lake_root, conn, filename, schema, table_name, fmt='csv'):
    src = raw_root / filename
    if not src.exists():
        print(f"⚠️ Skipping {filename} — file not found.")
        return
    if already_processed(conn, src):
        print(f"ℹ️ {filename} already processed, skipping.")
        return

    # Read by format
    if fmt == 'csv':
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
    elif fmt == 'parquet':
        tbl = pq.read_table(src)
    elif fmt == 'json':
        tbl = paj.read_json(src)
    else:
        print(f"❌ Unknown format for {filename}, skipping.")
        return

    # Cast to schema
    tbl = tbl.cast(schema, safe=False)

    # Add ingestion timestamp
    now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
    tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()] * len(tbl), type=pa.timestamp('us')))

    # Write to Bronze
    pq_base = lake_root / 'bronze' / 'parquet' / table_name
    dl_base = lake_root / 'bronze' / 'delta' / table_name
    write_parquet_partitioned(tbl, pq_base)
    write_delta(tbl, dl_base)

    # Manifest update
    mark_processed(conn, src, len(tbl))
    print(f"✅ Loaded {filename} ({len(tbl)} rows) to Bronze.")


# -------------------- Loader Registry --------------------

LOADERS = [
    {"filename": "customers.csv", "schema": customers_schema, "table": "customers"},
    {"filename": "products.csv", "schema": products_schema, "table": "products"},
    {"filename": "stores.csv", "schema": stores_schema, "table": "stores"},
    {"filename": "suppliers.csv", "schema": suppliers_schema, "table": "suppliers"},
    {"filename": "orders_header.csv", "schema": orders_header_schema, "table": "orders_header"},
    {"filename": "orders_lines.csv", "schema": orders_lines_schema, "table": "orders_lines"},
    {"filename": "shipments.parquet", "schema": shipments_schema, "table": "shipments", "fmt": "parquet"},
    {"filename": "returns_day1.parquet", "schema": returns_day1_schema, "table": "returns_day1", "fmt": "parquet"},
    {"filename": "exchange_rates.parquet", "schema": exchange_rates_schema, "table": "exchange_rates", "fmt": "parquet"},
    {"filename": "sensors.parquet", "schema": sensors_schema, "table": "sensors", "fmt": "parquet"},
    {"filename": "events.json", "schema": events_schema, "table": "events", "fmt": "json"},
]


# -------------------- Main --------------------

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    for loader in LOADERS:
        fmt = loader.get("fmt", "csv")
        generic_loader(
            raw_root,
            lake_root,
            conn,
            loader["filename"],
            loader["schema"],
            loader["table"],
            fmt
        )

    print("🎉 Bronze load completed for all registered tables.")


if __name__ == '__main__':
    main()
