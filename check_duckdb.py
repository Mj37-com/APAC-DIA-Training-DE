import duckdb

con = duckdb.connect(r'C:\Users\maustria\APAC-DIA-Training-DE\duckdb\warehouse.duckdb')
print("📋 Tables in DuckDB:")
print(con.execute("SHOW TABLES").fetchdf())