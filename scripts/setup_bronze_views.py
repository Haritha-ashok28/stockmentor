import duckdb

conn = duckdb.connect("dev.duckdb")
print(f"Successfully connected to dev.duckdb")

views = conn.sql("""
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_type = 'VIEW'  
""").fetchall()
for (view_name,) in views:
    conn.sql(f"DROP VIEW IF EXISTS {view_name}")
    print(f"Dropped old view: {view_name}")

domain = [
    "stock_prices",
    "company_info",
    "financials",
    "news"
]

for dataset in domain:
    conn.sql(f"""
        CREATE OR REPLACE VIEW {dataset} AS
        SELECT *
        FROM read_parquet(
            'bronze/{dataset}/**/*.parquet',
            hive_partitioning=true,
            union_by_name=true
        )
    """)
    print(f"created view: {dataset}")
conn.close()
print(f"All views created successfully. Connection closed")
