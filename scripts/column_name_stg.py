import duckdb
import pandas as pd

# Connect to DuckDB
conn = duckdb.connect('duckdb/warehouse.duckdb')  # adjust path if needed

# Get all tables with their schema
all_tables = conn.execute("SHOW TABLES").fetchdf()  # default only shows main
all_tables_with_schema = conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchdf()

print("All tables with schema:")
print(all_tables_with_schema)

# Filter only staging tables in the 'main_stg' schema
staging_tables_df = all_tables_with_schema[
    (all_tables_with_schema['table_schema'] == 'main_stg') &
    (all_tables_with_schema['table_name'].str.startswith('stg_'))
]

staging_tables = staging_tables_df['table_name'].tolist()
print("\nStaging tables detected:")
print(staging_tables)

if not staging_tables:
    print("\n⚠️ No staging tables found in main_stg.")
else:
    # Function to get column names for a table including schema
    def get_columns(schema_name, table_name):
        query = f"PRAGMA table_info('{schema_name}.{table_name}')"
        df = conn.execute(query).fetchdf()
        return df['name'].tolist()

    # Collect columns for all staging tables
    staging_columns = {
        table: get_columns('main_stg', table) for table in staging_tables
    }

    # Convert to DataFrame and save to CSV
    columns_df = pd.DataFrame(
        [(table, col) for table, cols in staging_columns.items() for col in cols],
        columns=['table_name', 'column_name']
    )
    columns_df.to_csv('staging_table_columns.csv', index=False)

    # Print nicely
    for table, columns in staging_columns.items():
        print(f"\n{table}:")
        print(columns)

    print("\n✅ Staging table columns saved to 'staging_table_columns.csv'")
