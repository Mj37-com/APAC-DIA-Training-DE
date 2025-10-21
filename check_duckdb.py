import duckdb
import pandas as pd

# Connect to your DuckDB database
conn = duckdb.connect('duckdb/warehouse.duckdb')

# Get all user tables (exclude system schemas)
tables = conn.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
""").fetchdf()

results = []

# Loop through each table and get row count + columns
for _, row in tables.iterrows():
    schema = row["table_schema"]
    table = row["table_name"]
    full_name = f"{schema}.{table}"

    try:
        # Row count
        count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]

        # Column names
        cols = conn.execute(f"DESCRIBE {full_name}").fetchdf()["column_name"].tolist()
        col_list = ", ".join(cols)

        results.append({
            "table_name": table,
            "row_count": count,
            "columns": col_list
        })
    except Exception as e:
        results.append({
            "table_name": table,
            "row_count": "Error",
            "columns": str(e)
        })

# Convert to DataFrame for pretty print
df_results = pd.DataFrame(results)

# Print neatly
print("\n📊 DuckDB Table Summary:")
print(df_results.to_string(index=False))

conn.close()
