import duckdb
con = duckdb.connect("duckdb/warehouse.duckdb")

# Inspect the table structure
print(con.execute("DESCRIBE main_stg.stg_stores").fetchdf())

# View the first few rows
print(con.execute("SELECT * FROM main_stg.stg_stores LIMIT 5").fetchdf())

con.close()
