import duckdb

db_path = "duckdb/warehouse.duckdb"
con = duckdb.connect(db_path)

# ✅ Get all schemas (works even if SHOW SCHEMAS not supported)
schemas = [
    s[0]
    for s in con.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()
    if not s[0].startswith('information_schema') and not s[0].startswith('pg_catalog')
]

for schema in schemas:
    tables = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'").fetchall()
    if not tables:
        continue

    print(f"\n🧹 Dropping {len(tables)} tables in schema '{schema}'...")
    for (t,) in tables:
        con.execute(f"DROP TABLE IF EXISTS {schema}.{t}")
        print(f"✅ Dropped: {schema}.{t}")

print("\n🎯 All tables in all schemas dropped successfully.")
con.close()
