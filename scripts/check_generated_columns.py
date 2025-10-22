import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

raw_dir = Path("scripts/data_raw")

def show_columns(file):
    ext = file.suffix
    if ext == ".csv":
        df = pd.read_csv(file, nrows=1)
    elif ext == ".parquet":
        df = pq.read_table(file).schema.names
        return file.name, df
    elif ext == ".json":
        import json
        with open(file) as f:
            data = json.load(f)
        # For JSON, just show top-level keys
        return file.name, list(data[0].keys()) if isinstance(data, list) else list(data.keys())
    else:
        return file.name, []

    return file.name, list(df.columns)

def main():
    print("📋 Column summary for generated files:\n")
    for f in sorted(raw_dir.glob("*")):
        name, cols = show_columns(f)
        print(f"🗂️ {name}")
        print("   →", ", ".join(cols))
        print()

if __name__ == "__main__":
    main()
