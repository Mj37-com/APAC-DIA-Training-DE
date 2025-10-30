import duckdb
import pandas as pd
from pathlib import Path

# Paths
DB_PATH = Path(r"C:\Program Files\Git\APAC-DIA-Training-DE\duckdb\warehouse.duckdb")
EXCEL_PATH = Path(r"C:\Program Files\Git\APAC-DIA-Training-DE\scripts\data_raw\exchange_rates.xlsx")

# Connect to DuckDB
con = duckdb.connect(str(DB_PATH))
con.execute("PRAGMA threads=4;")
con.execute("CREATE SCHEMA IF NOT EXISTS main;")

try:
    # 1️⃣ Read Excel via Pandas
    df = pd.read_excel(EXCEL_PATH)
    print("✅ Pandas read_excel() SUCCESS")
    print(df.head())

    # 2️⃣ Register temp DataFrame
    temp_name = "bronze_exchange_rates_temp"
    table_name = "bronze_exchange_rates"

    con.register(temp_name, df)

    # 3️⃣ Drop existing objects safely
    con.execute(f"DROP VIEW IF EXISTS {table_name};")
    con.execute(f"DROP TABLE IF EXISTS {table_name};")

    # 4️⃣ Create DuckDB table from DataFrame
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_name};")

    # 5️⃣ Verify result
    result = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    print(f"✅ {table_name} loaded successfully ({result:,} rows)")

except Exception as e:
    print("❌ Test FAILED:")
    print(e)

finally:
    con.close()
