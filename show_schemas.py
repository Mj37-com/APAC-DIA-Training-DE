import duckdb

# Connect to the DuckDB file
con = duckdb.connect('duckdb/warehouse.duckdb')

# Query all schemas
schemas = con.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()

print("Schemas in DuckDB:")
for schema in schemas:
    print(schema)
