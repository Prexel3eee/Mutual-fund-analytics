"""Verify Motilal Oswal database stats."""
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="mutual_fund_db",
    user="postgres",
    password="vivek"
)
cur = conn.cursor()

print("=" * 70)
print("MOTILAL OSWAL - FINAL DATABASE STATS")
print("=" * 70)

# Total stats
cur.execute("""
    SELECT COUNT(DISTINCT fm.fund_id) as funds,
           COUNT(DISTINCT ph.report_date) as dates,
           COUNT(*) as holdings
    FROM portfolio_holdings ph
    JOIN fund_master fm ON ph.fund_id = fm.fund_id
    JOIN amc_master am ON fm.amc_id = am.amc_id
    WHERE am.short_code = 'MOTILAL'
""")
row = cur.fetchone()
print(f"\n📊 Motilal Oswal Stats:")
print(f"   Unique Funds: {row[0]}")
print(f"   Report Dates: {row[1]} months")
print(f"   Total Holdings: {row[2]:,}")

# List report dates
cur.execute("""
    SELECT DISTINCT ph.report_date, COUNT(*) as holdings
    FROM portfolio_holdings ph
    JOIN fund_master fm ON ph.fund_id = fm.fund_id
    JOIN amc_master am ON fm.amc_id = am.amc_id
    WHERE am.short_code = 'MOTILAL'
    GROUP BY ph.report_date
    ORDER BY ph.report_date
""")
print(f"\n📅 Report Dates:")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]:,} holdings")

# Overall DB stats
print(f"\n" + "=" * 70)
print("OVERALL DATABASE STATS (ALL AMCs)")
print("=" * 70)

cur.execute("""
    SELECT am.amc_name, 
           COUNT(DISTINCT fm.fund_id) as funds,
           COUNT(DISTINCT ph.report_date) as dates,
           COUNT(*) as holdings
    FROM portfolio_holdings ph
    JOIN fund_master fm ON ph.fund_id = fm.fund_id
    JOIN amc_master am ON fm.amc_id = am.amc_id
    GROUP BY am.amc_name
    ORDER BY am.amc_name
""")
print(f"\n{'AMC':<30} {'Funds':<8} {'Months':<8} {'Holdings':<12}")
print("=" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<30} {row[1]:<8} {row[2]:<8} {row[3]:<12,}")

cur.execute("SELECT COUNT(*) FROM portfolio_holdings")
total = cur.fetchone()[0]
print(f"\n🎯 GRAND TOTAL: {total:,} holdings across all AMCs")

conn.close()
print("\n" + "=" * 70)
