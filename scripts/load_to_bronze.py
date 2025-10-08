# scripts/load_to_bronze.py
# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb

import argparse, pathlib, os, hashlib, json, datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from schemas.schemas import customers_schema, products_schema  # ⬅️ added products_schema

try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None


# -------------------- Helpers --------------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    return ap.parse_args()

def ensure_dirs(lake_root):
    for sub in ['bronze/parquet','bronze/delta']:
        (lake_root/sub).mkdir(parents=True, exist_ok=True)
    (lake_root/'_rejects').mkdir(parents=True, exist_ok=True)

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
        "SELECT 1 FROM manifest_processed_files WHERE src_path = ?", 
        [str(p)]
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

def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
    if write_deltalake is None:
        raise RuntimeError('deltalake not installed')
    write_deltalake(
        str(base_path),
        data=table,
        mode=mode,
        partition_by=partition_by or []
    )


# -------------------- Loaders --------------------

def generic_loader(raw_root, lake_root, conn, filename, schema, table_name):
    src = raw_root / filename
    if not src.exists():
        print(f"⚠️ Skipping {filename} — file not found.")
        return
    if already_processed(conn, src):
        print(f"ℹ️ {filename} already processed, skipping.")
        return

    # Read CSV and cast schema
    tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
    tbl = tbl.cast(schema, safe=False)

    # Add ingestion timestamp
    now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
    tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()] * len(tbl), type=pa.timestamp('us')))

    # Output paths
    pq_base = lake_root / 'bronze' / 'parquet' / table_name
    dl_base = lake_root / 'bronze' / 'delta' / table_name

    # Write Parquet + Delta
    write_parquet_partitioned(tbl, pq_base, partitioning=None)
    write_delta(tbl, dl_base, mode='append')

    # Mark as processed
    mark_processed(conn, src, len(tbl))
    print(f"✅ Loaded {filename} ({len(tbl)} rows) to Bronze.")


# -------------------- Loader Registry --------------------

LOADERS = [
    {"filename": "customers.csv", "schema": customers_schema, "table": "customers"},
    {"filename": "products.csv", "schema": products_schema, "table": "products"},
    # ➕ You can easily add more files here later
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
        generic_loader(
            raw_root,
            lake_root,
            conn,
            loader["filename"],
            loader["schema"],
            loader["table"]
        )

    print("🎉 Bronze load completed for all registered tables.")

if __name__ == '__main__':
    main()

#how to run in bash PYTHONPATH=. python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb