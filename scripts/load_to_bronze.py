import os
import argparse
import duckdb
import pandas as pd

def read_file(filepath):
    """Read CSV, Parquet, or JSON depending on file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        return pd.read_csv(filepath)
    elif ext == ".parquet":
        return pd.read_parquet(filepath)
    elif ext == ".json":
        return pd.read_json(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def load_to_bronze(raw_dir, lake_dir, manifest_path):
    os.makedirs(os.path.join(lake_dir, "bronze"), exist_ok=True)
    con = duckdb.connect(manifest_path)

    files_expected = [
        "customers.csv",
        "products.parquet",
        "stores.csv",
        "suppliers.csv",
        "orders_header.csv",
        "orders_lines.csv",
        "shipments.parquet",
        "returns_day1.parquet",
        "exchange_rates.parquet",
        "sensors.parquet",
        "events.json"
    ]

    for filename in files_expected:
        file_path = os.path.join(raw_dir, filename)
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            continue

        table_name = f"bronze_{os.path.splitext(filename)[0]}"
        output_path = os.path.join(lake_dir, "bronze", f"{os.path.splitext(filename)[0]}.parquet")

        try:
            df = read_file(file_path)
            print(f"📦 Reading {filename} ({len(df)} rows)...")

            # Try to read schema from manifest
            if con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]:
                existing_cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
                df_cols = df.columns.tolist()
                if set(existing_cols) != set(df_cols):
                    print(f"⚠️ Column mismatch detected for {filename}. Auto-aligning columns.")
                    for col in existing_cols:
                        if col not in df.columns:
                            df[col] = None
                    df = df[existing_cols]

            df.to_parquet(output_path, index=False)
            print(f"💾 Written to {output_path}")

            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM parquet_scan('{output_path}')")
            print(f"🧾 Table registered in DuckDB: {table_name}")

        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")

    con.close()
    print("✅ All raw data successfully loaded into Bronze zone.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", required=True, help="Path to raw data folder")
    parser.add_argument("--lake", required=True, help="Path to lake folder")
    parser.add_argument("--manifest", required=True, help="Path to DuckDB manifest file")
    args = parser.parse_args()

    load_to_bronze(args.raw, args.lake, args.manifest)

# reset and reload 
# rm duckdb/warehouse.duckdb
# PYTHONPATH=. python scripts/load_to_bronze.py --raw scripts/data_raw --lake lake --manifest duckdb/warehouse.duckdb
