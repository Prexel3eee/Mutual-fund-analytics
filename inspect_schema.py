"""Inspect PostgreSQL database schema."""
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="mutual_fund_db",
    user="postgres",
    password="vivek",
)

cur = conn.cursor()

# Get all tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
""")
tables = [r[0] for r in cur.fetchall()]

print("=== DATABASE TABLES ===")
for table in tables:
    print(f"\n{table}:")
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """)
    for col in cur.fetchall():
        nullable = "NULL" if col[2] == "YES" else "NOT NULL"
        print(f"  {col[0]}: {col[1]} ({nullable})")

conn.close()
