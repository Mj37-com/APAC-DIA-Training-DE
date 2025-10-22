import duckdb
import pandas as pd

# --- Connect to your DuckDB database ---
con = duckdb.connect("duckdb/warehouse.duckdb")

# --- Get all tables from all schemas ---
tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
""").fetchall()

# --- Collect row and column counts ---
summary_data = []

for schema, table in tables:
    full_table = f'"{schema}"."{table}"'
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        columns = con.execute(f"PRAGMA table_info({full_table})").fetchdf()
        col_count = len(columns)
    except Exception as e:
        row_count = "Error"
        col_count = "-"
    
    summary_data.append({
        "schema": schema,
        "table_name": table,
        "row_count": row_count,
        "column_count": col_count
    })

# --- Create DataFrame ---
df_summary = pd.DataFrame(summary_data)

# --- Display summary ---
print("\n📊 DuckDB Table Summary:\n")
print(df_summary.to_string(index=False))

# --- Save to CSV ---
df_summary.to_csv("duckdb_table_summary.csv", index=False)
print("\n✅ Summary saved to 'duckdb_table_summary.csv'")
