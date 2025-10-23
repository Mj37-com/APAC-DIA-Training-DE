import duckdb

db_path = "duckdb/warehouse.duckdb"
con = duckdb.connect(db_path)

# Get all tables
tables = con.execute("SHOW TABLES").fetchall()

print(f"🧹 Dropping {len(tables)} tables...")

for (t,) in tables:
    con.execute(f"DROP TABLE IF EXISTS {t}")
    print(f"✅ Dropped: {t}")

print("🎯 All tables dropped successfully.")
con.close()
