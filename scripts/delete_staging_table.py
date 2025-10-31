import duckdb

db_path = "duckdb/warehouse.duckdb"
con = duckdb.connect(db_path)

# Specify schema and table to drop
schema = "main_stg"
table_to_drop = "stg_orders_lines"

# Drop the table if it exists
con.execute(f"DROP TABLE IF EXISTS {schema}.{table_to_drop}")
print(f"✅ Dropped table: {schema}.{table_to_drop}")

con.close()
