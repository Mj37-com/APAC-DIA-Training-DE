import duckdb
import pandas as pd

# --- Connect to your DuckDB database ---
db_path = "duckdb/warehouse.duckdb"
con = duckdb.connect(db_path)

# --- Fetch all user tables from all schemas ---
tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
""").fetchall()

if not tables:
    print("⚠️ No tables found in the DuckDB database.")
    con.close()
    exit()

# --- Collect row and column counts ---
summary_data = []
for schema, table in tables:
    full_table = f'"{schema}"."{table}"'
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        col_count = len(con.execute(f"PRAGMA table_info({full_table})").fetchdf())
    except Exception as e:
        row_count, col_count = f"Error: {e}", "-"
    
    summary_data.append({
        "schema": schema,
        "table_name": table,
        "row_count": row_count,
        "column_count": col_count
    })

# --- Create summary DataFrame ---
df_summary = pd.DataFrame(summary_data)

# --- Display in terminal ---
print("\n📊 DuckDB Table Summary:\n")
print(df_summary.to_string(index=False))

# --- Save summary as CSV for easy checking ---
df_summary.to_csv("duckdb_table_summary.csv", index=False)
print(f"\n✅ Summary saved to 'duckdb_table_summary.csv'\n")

con.close()
