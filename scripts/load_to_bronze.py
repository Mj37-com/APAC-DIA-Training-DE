import os
import sys
import argparse
import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import time
import concurrent.futures
import warnings

# --- Suppress unnecessary warnings ---
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Auto-install tqdm if missing ---
try:
    from tqdm import tqdm
except ImportError:
    print("📦 Installing missing dependency: tqdm ...")
    os.system(f"{sys.executable} -m pip install tqdm --quiet")
    from tqdm import tqdm


# ---------- Robust JSONL reader for events ----------
def read_events_jsonl(filepath):
    """Read JSONL where each line is a JSON object with envelope + payload."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Flatten envelope + payload
                row = {**obj.get("envelope", {}), **obj.get("payload", {})}
                records.append(row)
            except json.JSONDecodeError as e:
                with open("load_errors.log", "a", encoding="utf-8") as log:
                    log.write(f"[{datetime.now()}] ❌ {filepath} line {i}: {e}\n")
    return pd.DataFrame(records)


# ---------- File Readers ----------
def read_file(filepath):
    """Read CSV, Parquet, Excel, JSON, or JSONL depending on file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext == ".csv":
            return pd.read_csv(filepath, low_memory=False)
        elif ext == ".parquet":
            return pd.read_parquet(filepath)
        elif ext in [".json", ".jsonl"]:
            if "events" in os.path.basename(filepath):
                return read_events_jsonl(filepath)
            else:
                try:
                    return pd.read_json(filepath, lines=True)
                except ValueError:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return pd.DataFrame(data)
        elif ext in [".xls", ".xlsx"]:
            return pd.read_excel(filepath, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        with open("load_errors.log", "a", encoding="utf-8") as log:
            log.write(f"[{datetime.now()}] ❌ {filepath}: {e}\n")
        return pd.DataFrame()


# ---------- Load + Merge ----------
def load_entity(entity_name, file_list, workers=8):
    """Merge all partitions of one entity into a single DataFrame."""
    dfs = []
    start_time = time.time()
    total_files = len(file_list)
    processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        with tqdm(total=total_files, desc=f"📥 {entity_name}", unit="file") as pbar:
            for df in executor.map(read_file, file_list):
                processed += 1
                pbar.update(1)
                elapsed = time.time() - start_time
                speed = processed / elapsed if elapsed > 0 else 0
                remaining = (total_files - processed) / speed if speed > 0 else 0
                pbar.set_postfix({
                    "Speed": f"{speed:.2f} f/s",
                    "ETA": f"{remaining:,.1f}s"
                })
                if not df.empty:
                    dfs.append(df)

    if not dfs:
        print(f"⚠️ {entity_name}: no valid data files found.")
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    print(f"✅ {entity_name}: {len(merged):,} rows × {len(merged.columns)} cols (merged from {total_files} files)")
    return merged


# ---------- Normalize datetime columns ----------
def normalize_datetime_columns(df):
    for col in df.columns:
        if df[col].dtype == object:
            # Try converting strings to datetime (ISO format)
            try:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
            except Exception:
                pass
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            # Ensure column is proper datetime64[ns]
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


# ---------- Main Loader ----------
def load_to_bronze(raw_dir, lake_dir, manifest_path, workers=8):
    raw_path = Path(raw_dir)
    lake_path = Path(lake_dir)
    bronze_path = lake_path / "bronze"
    bronze_path.mkdir(parents=True, exist_ok=True)

    entities = {
        "customers": ["customers.csv"],
        "products": ["products.csv"],
        "stores": ["stores.csv"],
        "suppliers": ["suppliers.csv"],
        "orders_header": ["orders_header", "order_dt"],
        "orders_lines": ["orders_lines"],
        "events": ["events_"],  # matches events_*.jsonl
        "sensors": ["sensors_", ".csv"],
        "exchange_rates": ["exchange_rates.xlsx"],
        "shipments": ["shipments.parquet"],
        "returns": ["returns_"],
    }

    con = duckdb.connect(manifest_path)
    summary = []
    job_start = time.time()

    print(f"\n🚀 Starting Bronze Load ({workers} workers)")
    print("=" * 60)

    for entity, patterns in entities.items():
        if entity == "events":
            all_files = [str(p) for p in raw_path.rglob("*.jsonl") if p.name.startswith(patterns[0])]
        else:
            all_files = [str(p) for p in raw_path.rglob("*") if all(pt in str(p) for pt in patterns)]

        if not all_files:
            print(f"⚠️ Skipping {entity}: no source files found.")
            continue

        df = load_entity(entity, all_files, workers=workers)
        if df.empty:
            print(f"⚠️ {entity} skipped (no valid data).")
            continue

        # --------- Normalize datetime columns ----------
        df = normalize_datetime_columns(df)

        output_path = bronze_path / f"bronze_{entity}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"💾 Written → {output_path}")

        con.execute(f"CREATE OR REPLACE TABLE bronze_{entity} AS SELECT * FROM parquet_scan('{output_path}')")
        summary.append((entity, len(df), len(df.columns)))

    con.close()

    total_time = time.time() - job_start
    print("\n📊 Bronze Layer Summary:")
    print("=" * 60)
    for e, rows, cols in summary:
        print(f"  • {e:<15} → {rows:>10,} rows × {cols:>3} cols")
    print(f"\n⏱️ Total Runtime: {total_time:,.1f} seconds")
    print("✅ All Bronze tables successfully created and registered in DuckDB.")
    print("📄 Errors (if any) are logged in load_errors.log")


# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load raw data into Bronze zone (auto-merge, skip errors, show ETA).")
    parser.add_argument("--raw", required=True, help="Path to raw data folder")
    parser.add_argument("--lake", required=True, help="Path to lake folder")
    parser.add_argument("--manifest", required=True, help="Path to DuckDB manifest file")
    parser.add_argument("--workers", type=int, default=8, help="Number of concurrent workers")
    args = parser.parse_args()

    load_to_bronze(args.raw, args.lake, args.manifest, args.workers)
