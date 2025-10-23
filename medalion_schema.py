import duckdb

con = duckdb.connect('./duckdb/warehouse.duckdb')

schemas = ['stg', 'silver', 'gold']
for s in schemas:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {s};")

con.close()
print("Schemas created successfully!")
