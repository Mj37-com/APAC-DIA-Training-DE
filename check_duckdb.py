import duckdb

# Connect to the correct dbt DuckDB file
conn = duckdb.connect('duckdb/warehouse.duckdb')

# List all schemas, tables, and views
tables = conn.execute("""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
""").fetchdf()

print(tables)
