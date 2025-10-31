import duckdb
con = duckdb.connect("duckdb/warehouse.duckdb")

# Inspect the table structure
print(con.execute("DESCRIBE main_stg.stg_customers").fetchdf())

# View the first few rows
print(con.execute("SELECT DISTINCT is_vip FROM main_stg.stg_customers").fetchdf())

con.close()
